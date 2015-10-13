#!/bin/env perl

use POSIX qw{};
use Getopt::Long;
use Pod::Usage;
$Getopt::Std::STANDARD_HELP_VERSION=1;

use strict;
use warnings;
use File::Basename;

my $script=undef;
my $reindex=1;
my $stageless=undef;
my $errstream=undef;
my $verbosity=1;
my $version="1.2.0";  # Must also update in POD block at bottom of file
my $progname="hpss-copy-scriptgen";
my $maxcmd=3000;
my $maxop=15;
my ($lsout,$srcpath,$tgtpath);

sub verbose {
  my $level=shift @_;
  if($level<=$verbosity) {
    warn(@_) if($level<0 || !defined($errstream));
    if(defined($errstream)) {
      my $datestamp=POSIX::asctime(localtime());
      chomp $datestamp;
      print($errstream "$datestamp: ",@_)
    }
  }
}

sub cmdify {
  my ($pre,$post,@args)=@_;
  my @ret;
  return '' if($#args<0);

  my $cmd="$pre";

  my $ops=0;
  foreach my $arg (@args) {
    if(length($arg)+length($cmd)+1+length($post)<$maxcmd &&
                ($maxop<1 || $ops<$maxop)) {
      $cmd="$cmd $arg";
      $ops++;
    } else {
      push @ret,"hsi -P -q '$cmd$post'\n";
      $cmd="$pre $arg";
      $ops=1;
    }
  }
  push @ret,"hsi -P -q '$cmd$post'\n";
  return join('',@ret);
}
sub djoin($$) {
  my ($d,$f)=@_;
  my $ret;
  if($d=~m(/$)) {
    $ret="$d$f"
  } else {
    $ret="$d/$f";
  }
  $ret=~s(//+)(/)g;
  return $ret;
}

sub VERSION_MESSAGE {
  print "hpss-copy-scriptgen version $version\n";
  exit(0);
}

sub parse_arguments_or_abort {
  # Note: globals are modified by this script:
  #
  #  $script - file descriptor for output script.
  #  $verbosity - verbosity level
  #  $maxcmd - maximum command length
  #  $maxop - maximum hsi sub-operations (excluding cd)
  #  $errstream - logging stream or undef to use stderr
  #  $reindex - flag for re-indexing HTAR archives (making *.tar.idx files)
  #  $stageless - flag for disabling "hsi stage"
  #  $lsout - George Vandenberghe's listing file
  #  $srcpath - source directory
  #  $tgtpath - target directory

  my($help,$man,$verbose,$quiet,$reindex,$optlogfile,$optstage);
  my($optmaxcmd,$optmaxop,$optscript,$opt_no_reindex,$optversion);
  Getopt::Long::Configure ("bundling");
  GetOptions('help|?'      => \&help,
             'man'         => \$man,
             'verbose|v+'  => \$verbose,
             'quiet|q'     => \$quiet,
             'n'           => \$opt_no_reindex,
             'logfile|e:s' => \$optlogfile,
             'm|maxcmd:i'  => \$optmaxcmd,
             'p|maxop:i'   => \$optmaxop,
             'o|script:s'  => \$optscript,
             'version'     => \$optversion,
             'stage!'      => \$optstage,
             ) or pod2usage(2);
  pod2usage(1) if $help;
  pod2usage(-exitstatus=>0,-verbose=>2) if $man;
  if($optversion) {
    VERSION_MESSAGE();
    exit(0);
  }

  # $verbosity - figure out verbosity level.
  $verbosity=$verbose+0 if defined($verbose) and $verbose>=0;
  $verbosity=0 if defined($quiet) && $quiet;

  # $errstream - decide logging location
  if(defined($optlogfile) && $optlogfile && $optlogfile ne "") {
    open($errstream,">>$optlogfile") 
      or die "$optlogfile: cannot open for append: $!\n";
    my $ofh=select $errstream;
    $|=1; # Do not buffer the log file.
    select $ofh;
  }

  # $script - decide output location.
  if(defined($optscript)) {
    verbose 0,"$optscript: send script here\n";
    open($script,">$optscript")
      or die "$optscript: cannot open for writing: $!\n";
  } else {
    $script=\*STDOUT;
  }

  # $stageless
  if(!defined($optstage)) {
    verbose 1,"Neither --stage nor --no-stage specified.  Will disable staging by default.\n";
    $stageless=1;
  } elsif($optstage) {
    verbose 1,"Staging requested ($optstage).  Will use a two-step stage/copy operation.\n";
    $stageless=0;
  } else {
    verbose 1,"Staging disabled ($optstage).  Will use stageless copies (copy -p -S).\n";
    $stageless=1;
  }

  # $maxop and $maxcmd
  if(defined($optmaxop)) {
    $maxop=$optmaxop;
    verbose 1,"Maximum HSI operations set to $maxop\n";
  }
  if(defined($optmaxcmd)) {
    $maxcmd=$optmaxcmd;
    verbose 1,"Maximum HSI command length changed to $maxcmd\n";
  }

  # $reindex - decide if reindexing is disabled (it is enabled by default)
  if(defined($opt_no_reindex) && $opt_no_reindex) {
    $reindex=0;
  }

  # Remaining three are from @ARGV:
  if ($#ARGV!=2) {
    # Incorrect number of arguments.  Abort with error message.
    pod2usage(2);
  }
  ($lsout,$srcpath,$tgtpath) = @ARGV;
  verbose 0,"Parsed ls -lRV output: $lsout\n";
  verbose 0,"Source HPSS path:      $srcpath\n";
  verbose 0,"Target HPSS path:      $tgtpath\n";
  return 1;
}

parse_arguments_or_abort();

########################################################################

my $basename=basename $srcpath;

$srcpath=~s(/+$)()g;
$tgtpath=~s(/+$)()g;
$srcpath=~s(//+)(/)g;
$tgtpath=~s(//+)(/)g;

$srcpath=~s(^/NCEPDEV/)(/NCEPDEV-ReadOnly/)g;

verbose 0,"List source files ($lsout)\n";
open(LSOUT,"<$lsout") or die "$lsout: cannot read: $!";
my @lsout=<LSOUT>;
close LSOUT;
chomp @lsout;

### List known source files
my (%archname,%archsize,%tapesize,%tapearch,%archdirset,%archdirs);
my %hassubdir;
my ($allsize,$allarch)=(0,0);
$archdirset{$tgtpath}=1;
sub receive_input_line {
    my ($size,$dir,$name,$pos,$tapebase)=@_;
    verbose 4,"$size - $dir - $name - $pos - $tapebase\n";
    my $tape=$tapebase;
    if($dir ne '/') {
      $name="$dir$name";
      my $adddir=$dir;
      $adddir=~s:^/+::g;
      $adddir=~s:/+$::g;
      $archdirset{$adddir}=1;
      my $parentdir=dirname($adddir);
      $hassubdir{$parentdir}=basename($adddir);
      verbose 2,"Directory $adddir with parent $parentdir\n"
    }
    $name=~s(^/+)()g;
    #verbose 4,"NAME NOW $name\n";
    push @{$archname{$tape}{$pos}},$name;
    push @{$archsize{$tape}{$pos}},$size+0;
    push @{$archdirs{$tape}{$pos}},$dir;
    $tapesize{$tape}+=$size+0;
    $tapearch{$tape}++;
    $allsize+=$size;
    $allarch++;
}
foreach my $lsout (@lsout) {
    next unless($lsout=~m(\Q$basename\E));
    unless($lsout=~m((?:TAPE|DISK)\s+(\d+)\s+.*\s\Q$srcpath\E(/(?:\S*/)?)([^/[:blank:]]+)\s.*Pos:\s*(\d+).*PV List: (.+?)\s*$)) {
        unless($lsout=~/\.idx\s+/) {
            verbose 3,"Non-matching line: $lsout\n";
        }
        next;
    }
    receive_input_line($1,$2,$3,$4,$5);
}

if($allarch<1) {
  die "No files match query.\n";
}

### List known target files:
my %tar=();
my %idx=();
my %havedir=();
my $lslr_from="hsi -P -q 'ls -lR $tgtpath' < /dev/null |";
verbose 0,"List target files ($lslr_from)\n";
if(open(LSLR,$lslr_from)) {
  my $dirname=undef;
  my $subdir=undef;
  my $line;
  my $subdir_warn=0;
  while(defined($line=<LSLR>)) {
    verbose 3,"LINE: $line";
    if($line =~ /^$tgtpath(\S*):\s*$/) {
      $subdir="$1";
      $subdir=~s:/+$::g;
      $subdir=~s:^/+::g;
      $dirname="$tgtpath/$subdir";
      verbose 2,"$subdir: scan directory $dirname\n";
      if(!defined($havedir{$subdir}) || !$havedir{$subdir}) {
        verbose 3,"$subdir: have dir\n";
        $havedir{$subdir}=1;
      }
      next;
    } elsif($line =~  m:^dr\S{8}\s+\S+\s+\S+\s+\S+\s+(\d+):) {
      # Subdirectory.
      next;
    } elsif($line=~/^\s*$/) {
      # Blank line;
      next;
    } elsif($line !~ m(^-r\S{8}\s+\S+\s+\S+\s+\S+\s+(\d+).*?([^/ \t]+?)(\.tar\.idx|\.tar)?\s*$)) {
      verbose 2,"Skip line: $line";
      next;
    }

    #next if /-temp-/;
    if(!defined($subdir) && $subdir_warn<10) {
      verbose -1,"Parser error: no directory specified in listing before $2$3";
      $subdir_warn++;
      if($subdir_warn==10) {
        verbose -1,"Will stop warning about parser issues, but something is very wrong";
      }
    }
    my ($tsize,$tbase,$text)=($1,$2,$3);
    $text='' unless defined $text;
    my $name="$tbase$text";
    my $tgtfile=djoin($subdir,"$tbase$text");
    $tgtfile=~s:^/+::g;
    if($text eq ".tar.idx") {
      $idx{$tgtfile}=$tsize;
      verbose 2,"Index   $tgtfile size $tsize\n";
    } elsif($text eq ".tar") {
      $tar{$tgtfile}=$tsize;
      verbose 2,"Archive $tgtfile size $tsize\n";
    } else {
      $tar{$tgtfile}=$tsize;
      verbose 2,"Non-HTAR file $tgtfile size $tsize\n";
    }
  }
} else {
  verbose -1,"$tgtpath: cannot list: $!.  Will assume target directory does not exist.\n";
}
close LSLR;

verbose 0,"Generate script.\n";

print $script "#!/bin/env bash

PS4='+\\d \\t: '    # place timestamp in every log line
set -x            # log all commands before running
set -u            # abort if we encounter an unset environment variable
#set -e            # uncomment to have script abort at first error

";

### Create mkdir commands
my @mkdirme=();
foreach my $dir (keys %archdirset) {
  verbose 2,"$dir: do we need to mkdir?\n";
  if($havedir{$dir}) {
    verbose 2,"$dir: exists; will not mkdir -p\n";
  } elsif(defined($hassubdir{$dir})) {
    verbose 1,"$dir: need to mkdir via subdirectory mkdir -p\n";
  } else {
    verbose 1,"$dir: mkdir -p\n";
    push @mkdirme,$dir;
  }
}
my $mkdir_count=$#mkdirme+1;
print $script "# Make $mkdir_count directories.\n";
if($#mkdirme>=0) {
  print $script cmdify("mkdir -p","",@mkdirme);
}

### Create staging and copying commands:
my ($files_left,$files_total)=(0,0);
my ($bytes_left,$bytes_total)=(0.0,0.0);
my ($tapes_left,$tapes_total)=(0,0);
my $partial_copy=0;
foreach my $tape (sort{$a cmp $b} keys %archname) {
  my $need_tape=0;
  my $request='';
  my @stageme=();
  my @copyme=();
  my @indexme=();
  my @remove=();
  my %copydirs=();
  my %copytgts=();

  foreach my $pos (sort{$a<=>$b} keys %{$archname{$tape}}) {
    my @names=@{$archname{$tape}{$pos}};
    my @sizes=@{$archsize{$tape}{$pos}};
    my @dirs=@{$archdirs{$tape}{$pos}};
    for (my $i=0;$i<=$#names;$i++) {
      my $name=$names[$i];
      my $size=$sizes[$i];
      my $dir=$dirs[$i];
      my $srcbase=basename $name;
      $name=~s:^\Q$dir\E/*::g;
      my $newname="$tgtpath/$name";

      $newname =~ s:/+:/:g;
      verbose 2,"$newname: $srcpath/$name => $tgtpath/$name\n";
      if ($newname =~ /\.tar.idx$/) {
        verbose 1,"$newname: skip index file\n";
        next;
      }
      if($size<=0) {
        verbose 1,"$name: empty; skip\n";
        next;
      }
      $bytes_total+=$size;
      $files_total++;
      if (defined($tar{$name}) && $tar{$name}==$size) {
        if($reindex) {
          if (defined($idx{$name}) && $idx{$name}>0) {
            verbose 2,"$name: done (tar size $tar{$name} index size $idx{$name}).\n";
          } elsif($name!~/\.tar$/) {
            verbose 2,"$name: not an HTAR archive, so not indexing.\n";
          } else {
            verbose 1,"$name: copied, but not indexed.  Will re-index.\n";
            $need_tape=1;
            if(defined($idx{$name})) {
              verbose 1,"$name: delete empty index file.\n";
              push @remove,"$newname.idx";
            }
            push @indexme,$newname;
          }
        }
      } elsif(defined($tar{$name})) {
        verbose 0,"warning: $name: partial copy ($tar{$name}!=$size); will re-copy.\n";
        $partial_copy++;
        $need_tape=1;
        $bytes_left+=$size;
        $files_left++;
        push @remove,"$newname";
        if($reindex && defined($idx{$name}) && $newname=~/\.tar$/) {
          verbose 0,"$name: delete index file.\n";
          push @remove,"$newname.idx";
        }
        push @stageme,$name;
        push @copyme,$name;
        $copytgts{$dir}=djoin($tgtpath,$dir);
        push @{$copydirs{$dir}},$srcbase;
        if($newname=~/\.tar$/) {
          push @indexme,$newname;
        }
      } else {
        # Never copied.
        if($reindex) {
          verbose 1,"$name: never copied; will copy file, and index it if it is an htar archive\n";
        } else {
          verbose 1,"$name: never copied; will copy file.\n";
        }
        $bytes_left+=$size;
        $need_tape=1;
        $files_left++;
        if($reindex && defined($idx{$name}) && $newname=~/\.tar$/) {
          verbose 0,"$name: delete index file for an archive we have not copied yet.\n";
          push @remove,"$newname.idx";
        }
        push @stageme,$name;

        $copytgts{$dir}=djoin($tgtpath,$dir);
        push @{$copydirs{$dir}},$srcbase;
        push @copyme,$name;
        if($newname=~/\.tar$/) {
          push @indexme,$newname;
        }
      }
   }
  }

  if($need_tape) {
    $tapes_left++;
  }
  $tapes_total++;

  print $script "\n# TAPE $tape: $tapearch{$tape} archive(s), total of $tapesize{$tape} bytes\n";
  verbose 1,"TAPE $tape: remove ".(1+$#remove)." stage ".(1+$#stageme)." copy ".(1+$#copyme)." index ".(1+$#indexme)."\n";
  if(!$need_tape) {
    verbose 1," - tape is not needed.\n";
  }
  my @copydirs=keys %copydirs;
  my $ndirs=1+$#copydirs;
  print $script cmdify("cd $srcpath ; rm","",@remove);
  my $dashS=" -S";
  if($stageless) {
    verbose 1,"Stageless copy enabled.  Will use copy -p -S instead of staging.\n";
  } else {
    print $script cmdify("cd $srcpath ; stage","",@stageme);
    $dashS="";
  }
  foreach my $dir (keys %copydirs) {
    my $srcdir=djoin($srcpath,$dir);
    my $tgtdir=djoin($tgtpath,$dir);
    print $script cmdify("cd $srcdir ; copy -p$dashS"," $tgtdir",@{$copydirs{$dir}});
  }
  if($reindex) {
    foreach my $newname (@indexme) {
      print $script "htar -Xf $newname\n";
    }
  }
}

my ($tb_total,$tb_left) = ($bytes_total/1e12,$bytes_left/1e12);
my $bytes_percent = 100-int(100 * $bytes_left/$bytes_total);  # NOTE: round down.
my $files_percent = 100-int(100 * $files_left/$files_total);
my $tapes_percent = 100-int(100 * $tapes_left/$tapes_total);
verbose 0,sprintf("%3d%% of bytes copied; still have %0.3fTB of %0.3fTB to go\n",
                  $bytes_percent,$tb_left,$tb_total);
verbose 0,sprintf("%3d%% of files copied; still have %d of %d to go\n",
                  $files_percent,$files_left,$files_total);
verbose 0,sprintf("%3d%% of tapes copied; still have %d of %d to go\n",
                  $tapes_percent,$tapes_left,$tapes_total);
if($partial_copy>1) {
  verbose -1,"WARNING: $partial_copy incomplete file copies detected.  They will be re-copied.\n";
} elsif($partial_copy==1) {
  verbose -1,"WARNING: $partial_copy incomplete file copy detected.  It will be re-copied.\n";
}

__END__

=head1 hpss-copy-scriptgen version 1.2.0

  hpss-copy-scriptgen - generates a bash script for copying data from one area of hpss to another.

=head1 SYNOPSIS

  hpss-copy-scriptgen [options] lsout srcdir tgtdir

Will generate a bash shell script to copy HTAR files recursively from
srcdir to tgtdir.  The shell script will go to stdout by default, but
a different location can be specified in -o.

 options:
  --help         Print short documentation and exit.
  --man          Browse full documentation in manpage viewer and exit.
  --version      Print version and exit.
  -e logfile     Set the log file location, instead of using stderr.
  -m 3000        Set the HSI maximum command length limit.
  -n             Disable HTAR re-indexing.
  -o output.sh   Set the output script name, instead of using stdout.
  -p 0           Maximum HSI sub-operations (files to copy, etc.) per command.
  -q             Be quiet (low verbosity).
  --stage        Stage files before copying (hsi stage, hsi copy -p)
  --no-stage     Do a stageless copy (hsi copy -p -S).  This is the default.
  -v             Be more verbose (use up to three times).
  --             Terminate option processing.

=head1 ARGUMENTS

=over 8

=item B<lsout>

One of George V's parsed hsi ls -lRV outputs.

=item B<srcdir>

HPSS source directory

=item B<tgtdir>

HPSS target directory

=back

=head1 OPTIONS

=over 2

=item B<-e logfile>

Set log file location (instead of stderr).  Certain critical error
messages will still be sent to stderr.  You may also see some
messages from HSI when it is run to list the output directory.

=item B<-m 3000>

Maximum HPSS command length in bytes.  Known limit as of this writing
was 3072.  It is advisable to set it slightly lower than that.

=item B<-n>

Disable HTAR re-indexing (don't make *.tar.idx files).  By default,
HTAR re-indexing is enabled: the script will run "htar -Xf" to
generate index files in the destination directory for any *.tar
archives.  If you enable this option, there will be NO *.tar.idx files
copied to the destination directory, even if there were ones in the
source.

=item B<-o output.sh>

Output location for the generated script.  By default, the script is
sent to stdout.

=item B<-p 15>

Maximum number of compound operations per hsi command.  This is the
files per hsi copy, files per hsi stage or directories per hsi mkdir.
Set to 0 for no limit.  The default, 15, should be safe.  Note that
the command length is still limited by the hsi command length limit,
set by the -m option.

=item B<-q>

Be quiet; only print warnings and final message.  This overrides the
-v option.

=item B<--stage, --no-stage>

Enable or disable staging of source archives.  The destination archive
is still staged; that is a limitation of HPSS.  The --no-stage option
eliminates a second copy of the archive, the source archive, on disk.
The default is to copy without staging.

With --no-stage (the default), the operation is:

    hsi copy -p -S source.tar target.tar
    htar -Xf target.tar

With --stage the operation is:

    hsi stage source.tar
    hsi copy -p source.tar target.tar
    htar -Xf target.tar

=item B<-v>

Be verbose.  Use multiple times (up to three) for more verbosity.  See
the VERBOSITY section for more information.

=item B<-->

Terminate option processing.  This is used to handle filenames
beginning with a dash.  For example, if the listing file is "-listing"
then:

    hpss-copy-scriptgen-v1.2.0.pl -- -listing /hpss/source/ /hpss/target/

=back

=head1 VERBOSITY

Meanings of various verbosity levels are below.  Verbose output is
cumulative; each verbosity level contains all output from all lower
verbosity levels.

=head2 Quiet Mode: -q

Warnings, errors and final summary only.  If nothing goes wrong, this
should be less than ten lines of text.  Warnings will inform you of
files that will be deleted and any partial transfers detected.

=head2 Default Verbosity

For each target file that has actions in the output script, one line
is shown per action explaining the rationalle.

=head2 Verbose Mode: -v

Adds additional logging explaining the steps of logic made by the
script.  There will be two lines of text per source file, and usually
two or three per target file.

=head2 Parser Diagnostic Mode: -v -v

Prints detailed debug information about every line of text read in by
the script and what the script plans on doing with it.

=head2 Super-Debug Mode: -v -v -v

Dumps all internal state information and other extensive debug
information not useful to general users.

=cut
