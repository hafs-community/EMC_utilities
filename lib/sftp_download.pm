#! /bin/env perl

package sftp_download;

use strict;
use warnings;

use File::Path qw{mkpath};
use Fcntl qw(S_ISDIR);
use Net::OpenSSH;
use Sys::Hostname;
use File::Basename;
use Net::SFTP::Foreign::Constants qw{SSH2_FXF_TRUNC SSH2_FXF_CREAT SSH2_FXF_WRITE};
use POSIX qw{mktime};

our $unix_epoch=mktime(0,0,0,1,0,70);

########################################################################

sub lockname   { dirname($_[1]) .'/temp/'.                      basename($_[1])  .'.lock' }
sub mylockname { dirname($_[1]) .'/temp/'.$_[0]->{trnsid}.'.'.  basename($_[1])  .'.ltmp' }
sub ltempname  { dirname($_[1]) .'/temp/'.$_[0]->{trnsid}.'.'.  basename($_[1])  .'.part' }

########################################################################

sub require_path {
    my ($self,$path)=@_;
    return 1 if(-d "$path");

    warn "Target $path exists and is a directory.\n";
    mkpath($path);

    if(! -d "$path") {
        die "Cannot make directory $path: $!\n";
    }
    return 1;
}

########################################################################

sub new {
    my %allowopt=(user=>1,port=>1,passwd=>1,password=>1,passphrase=>1,key_path=>1,
                  ctl_dir=>1,ssh_cmd=>1,scp_cmd=>1,rsync_cmd=>1,timeout=>1,
                  kill_ssh_on_timeout=>1,strict_mode=>1,async=>1,master_opts=>1,
                  default_stdin_fh=>1,default_stdout_fh=>1,default_stderr_fh=>1,
                  default_stdin_file=>1,default_stdout_file=>1,default_stderr_file=>1,
                  master_stdout_fh=>1,master_stderr_fh=>1,master_stdout_discard=>1,
                  master_stderr_discard=>1,expand_vars=>1,vars=>1,external_master=>1,
                  default_encoding=>1,default_stream_encoding=>1,
                  default_argument_encoding=>1);

    warn "got here";

    my ($class,$machine,$remote_source,$local_final,%opts)=@_;

    my $local_temp="$local_final/temp";
    my $remote_temp="$remote_source/temp";
    my $compress=0;

    if(defined($opts{remote_temp})) {
        $remote_temp=$opts{remote_temp};
        delete $opts{remote_temp};
    }

    if(defined($opts{local_temp})) {
        $local_temp=$opts{local_temp};
        delete $opts{local_temp};
    }

    if(defined($opts{compress}) && $opts{compress} =~ /\Ayes\z/i) {
        delete $opts{compress};
        $compress=1;
    }

    my (%newopt,$opt);
    foreach $opt(keys %opts) {
        if(defined $allowopt{$opt}) {
            $newopt{$opt}=$opts{$opt} ;
            warn "$opt = $newopt{$opt}\n";
        } else {
            warn "not sending $opt to Net::OpenSSH -> new\n";
        }
    }

    if($compress) {
        warn "Enabling ssh compression on master stream.\n";
        $newopt{master_opts}=['-C'];
    }

    my $ssh=Net::OpenSSH->new($machine,%newopt);
    my $sftp=$ssh->sftp();
    my $trnsid=hostname().'.'.sprintf("%x.%0hx",time(),rand(65536));
    
    my $self={machine=>$machine,       # name of remote machine
              rsrc=>$remote_source."/", # source directory on remote machine
              lfin=>$local_final."/",  # final directory on local machine
              trnsid=>$trnsid,
              rtemps=>{},               # list of remote temp directories
              ltmps=>{},
              sftpopts=>{%newopt},
              how=>"sftp", ssh=>$ssh, sftp=>$sftp,
              remote_temp=>$remote_temp,
              local_temp=>$local_temp};

    $self->{cleanup_maxage}=$opts{cleanup_maxage};
       
    bless($self,'sftp_download');

    warn "self = $self\n";
    
    return $self;
}

########################################################################

sub now {
    my $now=mktime(localtime())-$unix_epoch;
}

########################################################################

sub remote_stat {
    my ($self,$file,$reltime)=@_;

    $reltime=$self->now() unless defined($reltime);

    my $stat=$self->{sftp}->stat($file);
    my $mtime=$stat->mtime;
    my %statted=(
        size=>$stat->size, 
        mtime=>$mtime,
        age=>($reltime-$mtime),
    );
    return %statted;
}

########################################################################

sub not_ready {
    my ($self,$remote_file,%opts)=@_;
    my $now=time();
    warn "$remote_file: get size...\n";

    my %rstat=$self->remote_stat($remote_file);

    # Make sure the file exists and get its size.
    my $size=$rstat{size};
    if(!defined($size) || $size<0) {
        return "$remote_file: cannot determine size\n";
    }

    return "$remote_file: too small: $size<$opts{size_ge}"  if(defined($opts{size_ge}) && $size<$opts{size_ge});
    return "$remote_file: too large: $size>$opts{size_le}"  if(defined($opts{size_le}) && $size>$opts{size_le});
    return "$remote_file: too small: $size<=$opts{size_gt}" if(defined($opts{size_gt}) && $size<=$opts{size_gt});
    return "$remote_file: too small: $size>=$opts{size_ge}" if(defined($opts{size_lt}) && $size>=$opts{size_lt});
    return "$remote_file: has wrong size: $size!=$opts{size}" if(defined($opts{size}) && $size!=$opts{size});

    my $age=$rstat{age};
    if(!defined($age) || $age<0) {
        return "$remote_file: cannot determine age: $!\n";
    }

    return "$remote_file: too small: $age<$opts{age_ge}"  if(defined($opts{age_ge}) && $age<$opts{age_ge});
    return "$remote_file: too large: $age>$opts{age_le}"  if(defined($opts{age_le}) && $age>$opts{age_le});
    return "$remote_file: too small: $age<=$opts{age_gt}" if(defined($opts{age_gt}) && $age<=$opts{age_gt});
    return "$remote_file: too small: $age>=$opts{age_ge}" if(defined($opts{age_lt}) && $age>=$opts{age_lt});
    return "$remote_file: has wrong age: $age!=$opts{age}" if(defined($opts{age}) && $age!=$opts{age});

    return undef;
}

########################################################################

sub not_done {
    my ($self,$sftp_file,$local_file,%opts)=@_;

    # Make sure the file exists.
    my %rstat=$self->remote_stat($sftp_file);
    my $size=$rstat{size};

    return "$sftp_file: too small: $size<$opts{size_ge}"  if(defined($opts{size_ge}) && $size<$opts{size_ge});
    return "$sftp_file: too large: $size>$opts{size_le}"  if(defined($opts{size_le}) && $size>$opts{size_le});
    return "$sftp_file: too small: $size<=$opts{size_gt}" if(defined($opts{size_gt}) && $size<=$opts{size_gt});
    return "$sftp_file: too small: $size>=$opts{size_ge}" if(defined($opts{size_lt}) && $size>=$opts{size_lt});
    return "$sftp_file: has wrong size: $size!=$opts{size}" if(defined($opts{size}) && $size!=$opts{size});

    my $now=time();
    my $age=$rstat{age};

    my @stat=stat($local_file);
    if($#stat<0) {
        return "$local_file: cannot stat: $!\n";
    }
    my $lage=$stat[9]-$now;
    my $lsize=$stat[7];

    my $agediff=$lage-$age;
    my $sizediff=$size-$lsize;

    warn "$local_file: age=$age lage=$lage size=$size lsize=$lsize\n";

    return "$local_file: size diff too small: $sizediff<$opts{sizediff_ge}"  if(defined($opts{sizediff_ge}) && $sizediff>=$opts{sizediff_ge});
    return "$local_file: size diff too large: $sizediff>$opts{sizediff_le}"  if(defined($opts{sizediff_le}) && $sizediff<=$opts{sizediff_le});
    return "$local_file: size diff too small: $sizediff<=$opts{sizediff_gt}" if(defined($opts{sizediff_gt}) && $sizediff>$opts{sizediff_gt});
    return "$local_file: size diff too small: $sizediff>=$opts{sizediff_ge}" if(defined($opts{sizediff_lt}) && $sizediff<$opts{sizediff_lt});
    return "$local_file: has wrong size diff: $sizediff!=$opts{sizediff}" if(defined($opts{sizediff}) && $sizediff==$opts{sizediff});

    return "$local_file: age diff too small: $agediff<$opts{agediff_ge}"  if(defined($opts{agediff_ge}) && $agediff>=$opts{agediff_ge});
    return "$local_file: age diff too large: $agediff>$opts{agediff_le}"  if(defined($opts{agediff_le}) && $agediff<=$opts{agediff_le});
    return "$local_file: age diff too small: $agediff<=$opts{agediff_gt}" if(defined($opts{agediff_gt}) && $agediff>$opts{agediff_gt});
    return "$local_file: age diff too small: $agediff>=$opts{agediff_ge}" if(defined($opts{agediff_lt}) && $agediff<$opts{agediff_lt});
    return "$local_file: has wrong age diff: $agediff!=$opts{agediff}" if(defined($opts{agediff}) && $agediff!=$opts{agediff});

    return undef;
}

########################################################################

sub get {
    my ($self,$sftp_file,$local_file)=@_;

    die "Specify local file when calling get.  Aborting" unless defined $local_file;
    die "Specify remote (sftp) file when calling get.  Aborting" unless defined $sftp_file;

    # Make sure the sftp file exists and get the size and mtime.
    my %rstat=$self->remote_stat($sftp_file);
    my $rsize=$rstat{size};
    if(!defined($rsize)) {
        die "Unable to determine remote file size \"$sftp_file\": $!\n";
    }
    my $rmtime=$rstat{mtime};

    # Decide where to put the file.
    my $ftemp=$self->ltempname($local_file);

    # Make sure the parent directory exists.
    my %dirs=( dirname($ftemp)=>1, dirname($local_file)=>1 );
    my $dir;
    for $dir (keys %dirs) {
        $self->require_path($dir);
    }

    if(stat $ftemp) {
        unlink($ftemp);
        if(stat $ftemp) {
            die "Unable to remove local file \"$local_file\": $!\n";
        }
    }

    # Transfer the file.
    warn "Get $sftp_file => $ftemp\n";
    $self->{sftp}->get($sftp_file,$ftemp);
    warn "Back from get $sftp_file => $ftemp\n";

    # See if the transferred file exists.
    my @stat=stat($ftemp);
    if(!@stat) {
        unlink($ftemp);
        die "Unable to copy \"$self->{machine}:$sftp_file\" -> \"$ftemp\"\n";
    }

    # See if the size is right.
    my $lsize=$stat[7];
    if($lsize != $rsize) {
        unlink($ftemp);
        die "Error transferring file.  Local size = $lsize, remote size = $rsize.\n";
    }

    # Set the modification time.
    utime $rmtime,$rmtime,$ftemp;

    # Move to the final location.
    my $command="/bin/mv -f '$ftemp' '$local_file'";
    if(0!=system($command)) {
        unlink($ftemp);
        warn "Unable to move $ftemp to $local_file using command \"$command\"\n";
        return undef;
    }

    unlink($ftemp);

    my @xstat=stat($local_file);
    if(!@xstat) {
        warn "Unable to move \"$ftemp\" -> \"$local_file\" (cannot stat \"$local_file\" after move: $!)\n";
        return undef;
    }
    if($xstat[7]!=$rsize) {
        warn "Final moved file has the wrong size: size=$xstat[7], original=$rsize\n";
        return undef;
    }
    if($xstat[9]!=$rmtime) {
        warn "Final moved file has the wrong mtime: mtime=$xstat[9], original=$rmtime (seconds since beginning of Jan 1, 1970)\n";
        return undef;
    }

    return 1;
}

########################################################################

sub ignore_lock {
    my ($start,$end,$host,$trnsid)=(0,0,'invalid','invalid');
    return {host=>$host,trnsid=>$trnsid,start=>$start,end=>$end};
}

########################################################################

sub read_lock {
    my ($self,$lock)=@_;
    my @stat=stat($lock);
    if(@stat) {
        if(!open(LOCK,"< $lock")) {
            warn "$lock: cannot open for reading: $!\n";
            return undef;
        }
        my $lockcontents=<LOCK>;
        close LOCK;
        chomp $lockcontents;

        print "CONTENTS: ($lockcontents)\n";
        $lockcontents=~/^host "([^\"]*)" trnsid "([^\"]*)" start (\d+) end (\d+)/ or do {
            my $e=$self->{sftp}->error();
            warn "$lock: invalid format, so ignoring it.  SFTP says \"$e\"\n";
            return $self->ignore_lock();
        };
        my ($host,$trnsid,$start,$end)=($1,$2,$3,$4);
        $start=0 unless defined $start;
        $end=9e20 unless defined $end;
        $host='unknown' unless defined $host;
        $trnsid='unknown' unless defined $trnsid;
        return {host=>$host,trnsid=>$trnsid,start=>$start,end=>$end};
    } else {
        warn "$lock: cannot stat: $!\n";
    }
    return undef;
}

########################################################################

sub try_lock {
    my $now=time();
    my ($self,$local_file,$howlong)=@_;
    warn "local_file=\"$local_file\" howlong=\"$howlong\"\n";
    my $mylock=$self->mylockname($local_file);
    my $lock=$self->lockname($local_file);

    warn "mylock ($mylock) lock ($lock)\n";

    my %dirs=( dirname($mylock)=>1, dirname($lock)=>1 );
    my $dir;
    for $dir (keys %dirs) {
        $self->require_path($dir);
        $self->{ltemps}->{$dir}=1;
    }

    warn "$local_file: check lock...\n";

    my $locked=$self->read_lock($lock);

    if(ref $locked) {
        if($locked->{end}>$now) {
            warn "$local_file: already locked\n";
            return undef; # file is locked, lock has not expired
        } else {
            # lock has expired
            warn "$local_file: was locked, but lock expired\n";
            unlink($lock);
        }
    }

    warn "$local_file: create local temp file...\n";

    my $fh;
    unless(open($fh,"> $mylock")) {
        warn "$local_file: could not open local temporary file templock: $!\n";
        return undef;
    }
    $now=time();
    my $later=$now+$howlong;
    my $hostname=hostname();
    print $fh "host \"$hostname\" trnsid \"$self->{trnsid}\" start $now end $later\n";
    close $fh;

    warn "$local_file: check mylock file...\n";
    my $x=$self->read_lock($mylock);
    if(!defined($x)) {
        warn "$local_file: mylock not created (cannot read).\n";
        return undef; 
    }

    warn "$local_file: rename mylock file to lock file...\n";
    my $command="/bin/mv '$mylock' '$lock'";
    if(0!=system($command)) {
        unlink($mylock);
        warn "$local_file: unable to move $mylock to $lock using command \"$command\"\n";
        return undef;
    }

    warn "$local_file: check contents of lock file...\n";
    $x=$self->read_lock($lock);
    if(!defined($x)) {
        warn "$local_file: lock not created.\n";
        return undef; 
    }
    my %x=%{$x};
    if($x{host} eq $hostname && $x{trnsid} eq $self->{trnsid} && 
       $x{start}==$now && $x{end}==$later) {
        return 1;
    }
}

########################################################################

sub unlock {
    my $now=time();
    my ($self,$remote_file)=@_;

    my $mylock=$self->mylockname($remote_file);
    my $lock=$self->lockname($remote_file);

    warn "mylock ($mylock) lock ($lock)\n";

    unlink($mylock);

    my $locked=$self->read_lock($lock);
    if(!defined($locked)) {
        warn "File lock $lock does not exist.\n";
        return undef;
    }
    my %x=%{$locked};
    if($x{host} eq hostname && $x{trnsid} eq $self->{trnsid} && $now+10<$x{end}) {
        unlink($lock);
    }
}

########################################################################

sub cleanup {
    warn "cleanup";
    my $self=$_[0];
    my ($file,@delete);
    my $now=time();
    my $maxage=$self->{cleanup_maxage};
    $maxage=4500 unless defined $maxage;

    warn "maxage=$maxage";

    my ($ltmp,$dh,$deldir);
    foreach $ltmp (keys %{$self->{ltemps}}) {
        $deldir=1;
        opendir($dh,$ltmp);

        my @files=readdir($dh);
        
        foreach $file(@files) {
            my $fullfile="$ltmp/$file";
            my @stat=stat($fullfile);
            if($now-$stat[9]>$maxage && !S_ISDIR($stat[2])) {
                push(@delete,$fullfile);
            } elsif($file ne '.' && $file ne '..') {
                $deldir=0;
            }
        }
        closedir($dh);
        
        foreach $file (@delete) {
            my $fullfile="$file";
            warn "Delete local temp file $fullfile\n";
            unlink($fullfile);
        }

        if($deldir==1) {
            warn "Dir. $ltmp looks empty, so I'll rmdir it.  If it is not\n"
                ."empty, you should get an error here, and that's okay.\n";
            rmdir($ltmp) or warn "$ltmp: could not delete directory: $!\n";
        }
    }
}

########################################################################

sub process_file {
    my ($self,$remote_file,%opts)=@_;
    my $local_file=$opts{local};
    my $lock_time=300;

    if(defined($opts{lock_time})) {
        my $lt=$opts{lock_time}+0;
        if($lt>0) {
            if($lt<20) {
                $lock_time=20;
            } elsif($lt>3600) {
                $lock_time=3600;
            } else {
                $lock_time=$lt;
            }
        }
    }
    $lock_time=300           unless defined $lock_time;

    if(!defined($local_file)) {
        $local_file=basename($remote_file);
    }

    if($remote_file !~ /^\//) {
        # Remote path is relative, so prepend source directory
        warn "$remote_file: remote file is relative so prepending $self->{rsrc}\n";
        $remote_file=$self->{rsrc}.$remote_file;
    }

    if($local_file !~ /^\//) {
        # Local path is relative, so prepend target directory
        warn "$local_file: local file is relative so prepending $self->{lfin}\n";
        $local_file=$self->{lfin}.$local_file;
    }

    warn "remote_file=\"$remote_file\"\n";
    warn "local_file=\"$local_file\"\n";

    my $nr=$self->not_ready($remote_file,%opts);

    if(defined($nr)) {
        warn "$remote_file: $nr\n";
        return undef;
    }

    warn "$remote_file: ready.\n";

    my $nd=$self->not_done($remote_file,$local_file,%opts);

    if(defined($nd)) {
        warn "$local_file: $nd\n";
    } else {
        warn "$local_file: already transferred.\n";
        # File already transferred.
        return 1;
    }

    my $tl=$self->try_lock($local_file,$lock_time);
    if(! $tl) {
        warn "$local_file: try lock failed (".(defined($tl) ? $tl : 'undef').")\n";
        return undef;
    }
    warn "LOCKED.\n";

    eval { 
        $self->get($remote_file,$local_file);
    };
    my $q=$@;

    $self->unlock($local_file);

    if(! ($q eq '')) {
        warn "$q\n";
        return undef;
    }

    return 1;
}
