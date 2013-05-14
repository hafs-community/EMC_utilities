#! /bin/env perl

package ftp_download;

use strict;
use warnings;

use Net::FTP;
use Sys::Hostname;
use File::Basename;
use File::Path qw{mkpath};
use Fcntl qw(S_ISDIR);
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
    my %allow_opt=(firewall=>'Firewall',
                   firewall_type=>'FirewallType',block_size=>'BlockSize',
                   port=>'Port',timeout=>'Timeout',debug=>'Debug',
                   passive=>'Passive',hash=>'Hash',local_addr=>'LocalAddr');
    my ($class,$machine,$remote_source,$local_final,%opts)=@_;

    my $local_temp="$local_final/temp";
    if(defined($opts{local_temp})) {
        $local_temp=$opts{local_temp};
        delete $opts{local_temp};
    }

    my (%ftpopt,$opt,$ftpopt);
    foreach $opt(keys %opts) {
        if(defined($ftpopt=$opts{$opt})) {
            $ftpopt{$ftpopt}=$opts{$opt};
        }
    }

    my $ftp=Net::FTP->new($machine,%ftpopt);
    if(!$ftp) {
        die "Cannot establish FTP connection.  FTP says \"$@\".\n";
    }

    my $user=(defined($opts{user}) ? $opts{user} : 'anonymous');
    my $password=(defined($opts{password}) ? $opts{password} 
                  : "$ENV{USER}@".hostname());
    my $err=$ftp->login($user,$password);
    warn "err=\"$err\"\n";
    if(!$err) {
        die "Cannot log in to $machine via ftp, user $user.\n";
    }
    if(!$ftp->binary()) {
        die "Cannot switch to binary mode.\n";
    }

    my $trnsid=hostname().'.'.sprintf("%x.%0hx",time(),rand(65536));

    my $self={machine=>$machine,       # name of remote machine
              lfin=>$local_final."/",  # target local directory
              rsrc=>$remote_source."/",# source directory on remote machine
              ltemps=>{},              # temp directories used
              ftpopts=>{%ftpopt},
              user=>$user,
              password=>$password,
              want_close_reopen=>1,
              trnsid=>$trnsid,
              how=>"ftp", ftp=>$ftp};

    $self->{cleanup_maxage}=$opts{cleanup_maxage};
       
    bless($self,'ftp_download');

    return $self;
}

########################################################################

sub close_ftp {
    my $self=shift @_;

    if(!$self->{ftp}->close()) {
        warn "Unable to close FTP connection.  Will sleep three seconds and then call ftp->quit.\n";
        sleep 3;
        $self->{ftp}->quit();
    }

    undef $self->{ftp};
}

########################################################################

sub reopen {
    my $self=shift @_;
    my $ftp=Net::FTP->new($self->{machine},%{$self->{ftpopt}});
    if(!$ftp) {
        die "Cannot establish FTP connection.  FTP says \"$@\".\n";
    }

    my $user=(defined($self->{user}) ? $self->{user} : 'anonymous');
    my $password=(defined($self->{password}) ? $self->{password} 
                  : "$ENV{USER}@".hostname());
    my $err=$ftp->login($user,$password);
    warn "err=\"$err\"\n";
    if(!$err) {
        die "Cannot log in to $self->{machine} via ftp, user $user.\n";
    }
    if(!$ftp->binary()) {
        die "Cannot switch to binary mode.\n";
    }

    $self->{ftp}=$ftp;
}

########################################################################

sub remote_mtime {
    my ($self,$file)=@_;

    return $self->{ftp}->mdtm($file);
}

########################################################################

sub remote_age {
    my ($self,$file,$now)=@_;
    $now=$self->now() unless defined($now);

    my $mtime=$self->remote_mtime($file);
    if(!defined($mtime)) {
        warn "$file: cannot get remote mtime\n";
        return undef;
    }
    die "cannot determine current time(!?) $!" unless defined($now);

    my $age=$now-$mtime;

    return $age;
}

########################################################################

sub remote_size {
    my ($self,$file)=@_;

    my $size=$self->{ftp}->size($file);
    if(!defined($size)) {
        my @lines=$self->{ftp}->dir($file);
        if($#lines<0) {
            die "$file: cannot get size or list.\n";
            return undef;
        }
        my $line;
        foreach $line(@lines) {
            if($line =~ /^.{10}\s+\S+\s+\S+\s+\S+\s+(\d+)/) {
                return $1;
            } elsif(defined($line)) {
                chomp $line;
                warn "$file: unparsable line from DIR: \"$line\"\n";
            } else {
                warn "$file: discarding undef line from DIR.\n";
            }
        }
        warn "$file: SIZE failed, and DIR produced unparsable data.  Cannot determine size.\n";
    }
    return $size;
}

########################################################################

sub now {
    my $now=mktime(localtime())-$unix_epoch;
}

########################################################################

sub not_ready {
    my ($self,$remote_file,%opts)=@_;
    my $now=time();
    warn "$remote_file: get size...\n";

    # Make sure the file exists and get its size.
    my $size=$self->remote_size($remote_file);
    if(!defined($size) || $size<0) {
        return "$remote_file: cannot determine size\n";
    }

    return "$remote_file: too small: $size<$opts{size_ge}"  if(defined($opts{size_ge}) && $size<$opts{size_ge});
    return "$remote_file: too large: $size>$opts{size_le}"  if(defined($opts{size_le}) && $size>$opts{size_le});
    return "$remote_file: too small: $size<=$opts{size_gt}" if(defined($opts{size_gt}) && $size<=$opts{size_gt});
    return "$remote_file: too small: $size>=$opts{size_ge}" if(defined($opts{size_lt}) && $size>=$opts{size_lt});
    return "$remote_file: has wrong size: $size!=$opts{size}" if(defined($opts{size}) && $size!=$opts{size});

    my $age=$self->remote_age($remote_file);
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
    my ($self,$ftp_file,$local_file,%opts)=@_;

    # Make sure the file exists.
    my $size=$self->remote_size($ftp_file);
    if(!defined $size) {
        return "$ftp_file: cannot determine size.  File does not exist?  Connection broken?";
    }

    return "$ftp_file: too small: $size<$opts{size_ge}"  if(defined($opts{size_ge}) && $size<$opts{size_ge});
    return "$ftp_file: too large: $size>$opts{size_le}"  if(defined($opts{size_le}) && $size>$opts{size_le});
    return "$ftp_file: too small: $size<=$opts{size_gt}" if(defined($opts{size_gt}) && $size<=$opts{size_gt});
    return "$ftp_file: too small: $size>=$opts{size_ge}" if(defined($opts{size_lt}) && $size>=$opts{size_lt});
    return "$ftp_file: has wrong size: $size!=$opts{size}" if(defined($opts{size}) && $size!=$opts{size});

    my $now=time();
    my $age=$self->remote_age($ftp_file,$now);

    my @stat=stat($local_file);
    if($#stat<0) {
        return "$local_file: cannot stat: $!\n";
    }
    my $lage=$now-$stat[9];
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
    my ($self,$ftp_file,$local_file)=@_;

    die "Specify local file when calling get.  Aborting" unless defined $local_file;
    die "Specify remote (ftp) file when calling get.  Aborting" unless defined $ftp_file;

    # Make sure the ftp file exists and get the size and mtime.
    my $rsize=$self->remote_size($ftp_file);
    if(!defined($rsize)) {
        die "Unable to determine remote file size \"$ftp_file\": $!\n";
    }
    my $rmtime=$self->remote_mtime($ftp_file);

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
    warn "Get $ftp_file => $ftemp\n";
    $self->{ftp}->get($ftp_file,$ftemp);
    warn "Back from get $ftp_file => $ftemp\n";

    # See if the transferred file exists.
    my @stat=stat($ftemp);
    if(!@stat) {
        unlink($ftemp);
        die "Unable to copy \"$self->{machine}:$ftp_file\" -> \"$ftemp\"\n";
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

    # Make sure the connection is open.
    if(!defined($self->{ftp})) {
        $self->reopen();
    }

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
