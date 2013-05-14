#! /bin/env perl

package sftp_upload;

use strict;
use warnings;

use File::Path qw{mkpath};
use Fcntl qw(S_ISDIR);
use Net::OpenSSH;
use Sys::Hostname;
use File::Basename;
use Net::SFTP::Foreign::Constants qw{SSH2_FXF_TRUNC SSH2_FXF_CREAT SSH2_FXF_WRITE};

########################################################################

sub lockname   { dirname($_[1]) .'/temp/'.                      basename($_[1])  .'.lock' }
sub mylockname { dirname($_[1]) .'/temp/'.$_[0]->{trnsid}.'.'.  basename($_[1])  .'.ltmp' }
sub rtempname  { dirname($_[1]) .'/temp/'.$_[0]->{trnsid}.'.'.  basename($_[1])  .'.part' }
sub lworkname  { $_[0]->{local_temp}.$_[0]->{trnsid}.'.'.  basename($_[1])  .'.temp' }

########################################################################

sub require_path {
    my ($self,$path)=@_;
    my $f=$self->{sftp}->stat($path);
    if(defined($f)) {
        if(!S_ISDIR($f->perm)) {
            die "Remote target $path exists but is not a directory.\n";
        } else {
            warn "Target $path exists and is a directory.\n";
        }
    } else {
        warn "Target $path does not exist so I will make it.\n";
        $self->{sftp}->mkpath($path);
    }
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
    
    my ($class,$machine,$remote_final,%opts)=@_;
    warn "class is $class\n";
    my $local_temp="/tmp";
    my $remote_temp="$remote_final/temp";
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

    warn "machine=($machine) in new\n";

    my (%newopt,$opt);
    foreach $opt(keys %opts) {
        if(defined $allowopt{$opt}) {
            $newopt{$opt}=$opts{$opt} ;
            warn "$opt = $newopt{$opt}\n";
        } else {
            warn "not sending $opt to Net::OpenSSH -> new\n";
        }
    }

    if( defined($local_temp) && ! -z $local_temp && ! -d $local_temp) {
        mkpath($local_temp);
    }

    if($compress) {
        warn "Enabling ssh compression on master stream.\n";
        $newopt{master_opts}=['-C'];
    }

    my $ssh=Net::OpenSSH->new($machine,%newopt);
    my $sftp=$ssh->sftp();

    my $trnsid=hostname().'.'.sprintf("%x.%0hx",time(),rand(65536));
    
    my $self={machine=>$machine,       # name of remote machine
              rfin=>$remote_final."/", # final directory on remote machine
              trnsid=>$trnsid,
              rtemps=>{},               # list of remote temp directories
              how=>"sftp", ssh=>$ssh, sftp=>$sftp,
              local_temp=>$local_temp};

    $self->{cleanup_maxage}=$opts{cleanup_maxage};
       
    bless($self,'sftp_upload');

    warn "self = $self\n";
    
    return $self;
}

########################################################################

sub not_ready {
    my ($self,$local_file,%opts)=@_;
    warn "$local_file: stat...\n";
    # Make sure the file exists.
    my @stat=stat($local_file);
    if($#stat<0) {
        return "$local_file: cannot stat: $!\n";
    }

    return "$local_file: too small: $stat[7]<$opts{size_ge}"  if(defined($opts{size_ge}) && $stat[7]<$opts{size_ge});
    return "$local_file: too large: $stat[7]>$opts{size_le}"  if(defined($opts{size_le}) && $stat[7]>$opts{size_le});
    return "$local_file: too small: $stat[7]<=$opts{size_gt}" if(defined($opts{size_gt}) && $stat[7]<=$opts{size_gt});
    return "$local_file: too small: $stat[7]>=$opts{size_ge}" if(defined($opts{size_lt}) && $stat[7]>=$opts{size_lt});
    return "$local_file: has wrong size: $stat[7]!=$opts{size}" if(defined($opts{size}) && $stat[7]!=$opts{size});

    my $age=time()-$stat[9];

    return "$local_file: too new: $age<$opts{age_ge}"  if(defined($opts{age_ge}) && $age<$opts{age_ge});
    return "$local_file: too old: $age>$opts{age_le}"  if(defined($opts{age_le}) && $age>$opts{age_le});
    return "$local_file: too new: $age<=$opts{age_gt}" if(defined($opts{age_gt}) && $age<=$opts{age_gt});
    return "$local_file: too old: $age>=$opts{age_ge}" if(defined($opts{age_lt}) && $age>=$opts{age_lt});
    return "$local_file: has wrong age: $age!=$opts{age}" if(defined($opts{age}) && $age!=$opts{age});

    return undef;
}

########################################################################

sub not_done {
    my ($self,$local_file,$remote_file,%opts)=@_;

    # Make sure the file exists.
    my $stat=$self->{sftp}->stat($remote_file);
    if(!defined $stat) {
        return "$remote_file: cannot stat.  SFTP says \"".$self->{sftp}->error."\"\n";
    }

    my $size=$stat->size;

    return "$remote_file: too small: $size<$opts{size_ge}"  if(defined($opts{size_ge}) && $size<$opts{size_ge});
    return "$remote_file: too large: $size>$opts{size_le}"  if(defined($opts{size_le}) && $size>$opts{size_le});
    return "$remote_file: too small: $size<=$opts{size_gt}" if(defined($opts{size_gt}) && $size<=$opts{size_gt});
    return "$remote_file: too small: $size>=$opts{size_ge}" if(defined($opts{size_lt}) && $size>=$opts{size_lt});
    return "$remote_file: has wrong size: $size!=$opts{size}" if(defined($opts{size}) && $size!=$opts{size});

    my @stat=stat($local_file);
    if($#stat<0) {
        return "$local_file: cannot stat: $!\n";
    }
    my $now=time();
    my $age=$now-$stat->mtime;

    my $lsize=$stat[7];
    my $lage=$now-$stat[9];

    my $agediff=$age-$lage;
    my $sizediff=$lsize-$size;

    warn "$remote_file: age=$age lage=$lage size=$size lsize=$lsize\n";

    return "$remote_file: size diff too small: require $sizediff<$opts{sizediff_ge}"  if(defined($opts{sizediff_ge}) && $sizediff>=$opts{sizediff_ge});
    return "$remote_file: size diff too large: require $sizediff>$opts{sizediff_le}"  if(defined($opts{sizediff_le}) && $sizediff<=$opts{sizediff_le});
    return "$remote_file: size diff too small: require $sizediff<=$opts{sizediff_gt}" if(defined($opts{sizediff_gt}) && $sizediff>$opts{sizediff_gt});
    return "$remote_file: size diff too small: require $sizediff>=$opts{sizediff_ge}" if(defined($opts{sizediff_lt}) && $sizediff<$opts{sizediff_lt});
    return "$remote_file: has wrong size diff: require $sizediff!=$opts{sizediff}" if(defined($opts{sizediff}) && $sizediff==$opts{sizediff});

    return "$remote_file: age diff too small: require $agediff<$opts{agediff_ge}"  if(defined($opts{agediff_ge}) && $agediff>=$opts{agediff_ge});
    return "$remote_file: age diff too large: require $agediff>$opts{agediff_le}"  if(defined($opts{agediff_le}) && $agediff<=$opts{agediff_le});
    return "$remote_file: age diff too small: require $agediff<=$opts{agediff_gt}" if(defined($opts{agediff_gt}) && $agediff>$opts{agediff_gt});
    return "$remote_file: age diff too small: require $agediff>=$opts{agediff_ge}" if(defined($opts{agediff_lt}) && $agediff<$opts{agediff_lt});
    return "$remote_file: has wrong age diff: require $agediff!=$opts{agediff}" if(defined($opts{agediff}) && $agediff!=$opts{agediff});


    return undef;
}



########################################################################

sub put {
    my ($self,$local_file,$remote_file)=@_;

    die "Specify local file when calling put.  Aborting" unless defined $local_file;
    die "Specify remote file when calling put.  Aborting" unless defined $remote_file;

    # Make sure the local file exists.
    my @stat=stat($local_file);
    if(!@stat) {
        die "Unable to stat local file \"$local_file\": $!\n";
    }

    # Decide where to put the file.
    my $ftemp=$self->rtempname($remote_file);

    # Make sure the parent directory exists.
    my $rtemp=dirname($remote_file);
    $self->require_path($rtemp);

    # Transfer the file.
    warn "Copying $local_file => $ftemp ...\n";
    $self->{sftp}->put($local_file,$ftemp,late_set_perm => 1);
    warn "  ... returned from sftp put.\n";

    # See if the transferred file exists.
    my $rstat=$self->{sftp}->stat($ftemp);
    if(!defined($rstat)) {
        die "Unable to copy \"$local_file\" -> \"$self->{machine}:$ftemp\": ".$self->{sftp}->error."\n";
    }

    # See if the size is right.
    my $rsize=$rstat->size();
    if($rsize != $stat[7]) {
        $self->{sftp}->remove($ftemp);
        die "Error transferring file.  Local size = $stat[7], remote size = $rsize.\n";
    }

    # Move to the final location.
    if(!$self->{sftp}->atomic_rename($ftemp,$remote_file)) {
#        warn "Unable to move \"$self->{machine}:$ftemp\" to \"$self->{machine}:$remote_file\" using sftp atomic_rename.  Will try rename.  SFTP says \"".$self->{sftp}->error."\"\n";
        unless($self->{sftp}->rename($ftemp,$remote_file)) {
#            warn "Unable to move \"$self->{machien}:$ftemp\" to \"$self->{machine}:$remote_file\" using rename.  SSH says $?\n";
            unless($self->{ssh}->system("/bin/mv -f '$ftemp' '$remote_file'")) {
                warn "Unable to move \"$self->{machine}:$ftemp\" to \"$self->{machine}:$remote_file\" using ssh mv -f, sftp rename or sftp atomic rename.  SSH says $?\n";
                $self->{sftp}->remove($ftemp);
                return undef;
            }
        }
    }
    
    # See if the transferred file exists.
    $rstat=$self->{sftp}->stat($remote_file);
    if(!defined($rstat)) {
        $self->{sftp}->remove($ftemp);
        warn "Unable to move \"$self->{machine}:$ftemp\" -> \"$self->{machine}:$remote_file\": ".$self->{sftp}->error."\n";
        return undef;
    }

    # See if the size is right.
    $rsize=$rstat->size();
    if($rsize != $stat[7]) {
        $self->{sftp}->remove($ftemp);
        warn "Unable to move \"$self->{machine}:$ftemp\" -> \"$self->{machine}:$remote_file\": ".$self->{sftp}->error."\n";
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
    my $stat=$self->{sftp}->stat($lock);
    if(defined($stat)) {
        my $lockcontents=$self->{sftp}->get_content($lock);
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
        my $e=$self->{sftp}->error();
        warn "$lock: cannot stat, so lock file probably does not exist.  SFTP says \"$e\"\n";
        return $self->ignore_lock();
    }
    return undef;
}

########################################################################

sub try_lock {
    my $now=time();
    my ($self,$remote_file,$howlong)=@_;

    my $mylock=$self->mylockname($remote_file);
    my $lock=$self->lockname($remote_file);

    # Make sure the lock files' directories exist.
    my $dir;
    my %req=( dirname($mylock)=>1, dirname($lock)=>1 );
    foreach $dir (keys %req) {
        $self->require_path($dir);
        $self->{rtemps}->{$dir}=1;
    }

    warn "mylock ($mylock) lock ($lock)\n";

    warn "$remote_file: check lock...\n";

    my $locked=$self->read_lock($lock);

    if(ref $locked) {
        if($locked->{end}>$now) {
            warn "$remote_file: already locked\n";
            return undef; # file is locked, lock has not expired
        } else {
            # lock has expired
            warn "$remote_file: was locked, but lock expired\n";
            $self->{sftp}->remove($lock);
        }
    }

    warn "$remote_file: create local temp file...\n";

    my $fh;
    unless(open($fh,"> templock")) {
        warn "$remote_file: could not open local temporary file templock: $!\n";
        return undef;
    }
    $now=time();
    my $later=$now+$howlong;
    my $hostname=hostname();
    print $fh "host \"$hostname\" trnsid \"$self->{trnsid}\" start $now end $later\n";
    close $fh;

    warn "$remote_file: copy temp file to remote mylock file...\n";
    my $p=$self->{sftp}->put('templock',"$mylock");
    if(!$p) {
        warn "$mylock: could not copy templock to \"$self->{machine}:$mylock\".  SFTP says \"".$self->{sftp}->error()."\"\n";
    }

    warn "$remote_file: stat remote mylock file...\n";
    my $q=$self->{sftp}->stat($mylock);
    if(!defined($q)) {
        warn "$mylock: could not create lock (cannot stat).\n";
        die "$mylock: could not create lock (cannot stat).\n";
    }

    warn "$remote_file: check remote mylock file...\n";
    my $x=$self->read_lock($mylock);
    if(!defined($x)) {
        warn "$remote_file: mylock not created (cannot read).\n";
        return undef; 
    }

    warn "$remote_file: rename mylock file to lock file...\n";
    $self->{sftp}->rename($mylock,$lock);

    warn "$remote_file: stat lock file...\n";
    $q=$self->{sftp}->stat($lock);

    warn "$remote_file: check contents of lock file...\n";
    $x=$self->read_lock($lock);
    if(!defined($x)) {
        warn "$remote_file: lock not created.\n";
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

    $self->{sftp}->remove($mylock);

    my $locked=$self->read_lock($lock);
    if(!defined($locked)) {
        warn "File lock $lock does not exist.\n";
        return undef;
    }
    my %x=%{$locked};
    if($x{host} eq hostname && $x{trnsid} eq $self->{trnsid} && $now+10<$x{end}) {
        $self->{sftp}->remove($lock);
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

    my $deldir;
    my $rtmp;
    foreach $rtmp (keys %{$self->{rtemps}}) {
        $deldir=1;
        my $dh=$self->{sftp}->opendir($rtmp);
        
        my @files=$self->{sftp}->readdir($dh);
        
        foreach $file(@files) {
            if($now-$file->{a}->mtime()>$maxage &&
               !S_ISDIR($file->{a}->perm())) {
                push(@delete,$file);
            } elsif ($file ne '.' && $file ne '..') {
                $deldir=0;
            }
        }
        $self->{sftp}->closedir($dh);
        
        foreach $file (@delete) {
            warn "Delete remote temp file $rtmp/$file->{filename}\n";
            $self->{sftp}->remove($rtmp."/".$file->{filename});
        }

        if($deldir==1) {
            warn "Dir. $rtmp looks empty, so I'll rmdir it.  If it is not\n"
                ."empty, you should get an error here, and that's okay.\n";
            $self->{sftp}->rmdir($rtmp);
        }
    }
}

########################################################################

sub process_file {
    my ($self,$local_file,%opts)=@_;
    my $remote_file=$opts{remote};
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

    if(!defined($remote_file)) {
        $remote_file=basename($local_file);
    }

    if($remote_file !~ /^\//) {
        # Remote path is relative, so prepend target directory
        $remote_file=$self->{rfin}.$remote_file;
    }

    my $nr=$self->not_ready($local_file,%opts);

    if(defined($nr)) {
        warn "$local_file: $nr\n";
        return undef;
    }

    my $nd=$self->not_done($local_file,$remote_file,%opts);

    if(defined($nd)) {
        warn "$local_file: $nd\n";
    } else {
        warn "$local_file: already transferred.\n";
        # File already transferred.
        return 1;
    }

    my $tl=$self->try_lock($remote_file,$lock_time);
    if(! $tl) {
        warn "$local_file: try lock failed (".(defined($tl) ? $tl : 'undef').")\n";
        return undef;
    }
    warn "LOCKED.\n";

    if(defined($opts{subset}) && $opts{subset} ne '') {
        warn "SUBSETTING REQUESTED: $opts{subset}\n";
        my $intermediate=$self->lworkname($local_file);
        eval { 
            warn "( set INFILE=$local_file OUTFILE=$intermediate )";
            $ENV{INFILE}=$local_file;
            $ENV{OUTFILE}=$intermediate;
            warn "SYSTEM($opts{subset})\n";
            system($opts{subset});
            $self->put($intermediate,$remote_file);
        };
        unlink($intermediate);
    } else {
        warn "Not subsetting.\n";
        eval { 
            $self->put($local_file,$remote_file);
        };
    }
    my $q=$@;

    $self->unlock($remote_file);

    if(! ($q eq '')) {
        warn "$q\n";
        return undef;
    }

    return 1;
}


1;
