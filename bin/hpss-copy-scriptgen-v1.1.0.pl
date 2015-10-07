#!/bin/env perl

use Getopt::Std;
$Getopt::Std::STANDARD_HELP_VERSION=1;

use strict;
use warnings;
use File::Basename;

my $verbosity=1;
my $version="1.1.0";
my $progname="hpss-copy-scriptgen";

sub verbose {
  my $level=shift @_;
  if($level<=$verbosity) {
    warn @_;
  }
}

sub cmdify {
  my ($pre,$post,@args)=@_;
  my @ret;
  return '' if($#args<0);

  my $cmd="$pre";

  foreach my $arg (@args) {
    if(length($arg)+length($cmd)+1+length($post)<3000) {
      $cmd="$cmd $arg";
    } else {
      push @ret,"hsi -P -q '$cmd$post'\n";
      $cmd="$pre $arg";
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

sub VERSION_MESSAGE {  ## do not change name; expected by getopt
  print "make-hpss-copy-recrusive.pl version $version\n";
}

my $maxcmd=3000;

sub HELP_MESSAGE {  ## do not change name; expected by getopt
  my $error=shift @_;
  my $message="$progname version $version
Usage: 
    $progname [options] lsout srcdir tgtdir

Will generate a shell script to copy HTAR files recursively from
srcdir to tgtdir.  The shell script will go to stdout by default, but
a different location can be specified in -o.

Arguments:
  lsout -- one of George V's hsi ls outputs
  srcdir -- HPSS source directory
  tgtdir -- HPSS target directory

Options:
  -v = be verbose
  -q = only print warnings and final message
  -o output.sh = output script location
  -n = disable HTAR re-indexing (don't make *.tar.idx files)
  -m $maxcmd = maximum HPSS command length in bytes
  -- = terminate option processing (use to handle filenames beginning
        with a dash)
";

  if(defined($error) && length($error)>5) {
    print STDERR $message.$error."\n";
  } else {
    print $message;
  }
}

my %opts;
getopts('Svqno:m:',\%opts);

if ($#ARGV!=2) {
  # Incorrect number of arguments.  Abort with error message.
  HELP_MESSAGE("SCRIPT IS ABORTING: Exactly three arguments expected.");
  exit 2;
}

my $script=undef;
my $reindex=1;
my $lsout=$ARGV[0];
my $srcpath=$ARGV[1];
my $tgtpath=$ARGV[2];

$reindex = ! (defined($opts{n}) && $opts{n});
if(defined($opts{q}) && $opts{q}) {
  $verbosity=0;
}
if(defined($opts{v}) && $opts{v}) {
  $verbosity++;  # instead of =2, to ensure -q and -v cancel out
}
if(defined($opts{S}) && $opts{S}) {
  $verbosity=4;
  verbose 4,"Secret super-verbose (-S) option enabled.\n";
}
if(defined($opts{o})) {
  verbose 0,"$opts{o}: send script here\n";
  open($script,">$opts{o}") or die "$opts{o}: cannot open for writing.\n";
} else {
  $script=\*STDOUT;
}
if(!$reindex) {
  verbose 0,"Disable HTAR re-indexing due to -n option (no *.tar.idx files).\n";
}

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
foreach my $lsout (@lsout) {
    next unless($lsout=~m(\Q$basename\E));
#/NCEPDEV-ReadOnly/hpssuser/g01/hurpara/jet-2013-hwrf-parallel/hhs/parallel-20130913-132005-hhs.tar
    unless($lsout=~m((?:TAPE|DISK)\s+(\d+)\s+.*\s\Q$srcpath\E(/(?:\S*/)?)([^/[:blank:]]+)\s.*Pos:\s*(\d+).*PV List: (.+?)\s*$)) {
        unless($lsout=~/\.idx\s+/) {
            verbose 2,"Bad line: $lsout\n";
        }
        next;
    }
    #next if $lsout=~/-temp-/;
    my ($size,$dir,$name,$pos,$tapebase)=($1,$2,$3,$4,$5);
    #verbose 1,"$size - $dir - $name - $pos - $tapebase\n";
    my $tape=$tapebase;
    if($dir ne '/') {
      $name="$dir$name";
      my $adddir=$dir;
      $adddir=~s:^/+::g;
      $adddir=~s:/+$::g;
      $archdirset{$adddir}=1;
      my $parentdir=dirname($adddir);
      $hassubdir{$parentdir}=basename($adddir);
    }
    $name=~s(^/+)()g;
    #verbose 1,"NAME NOW $name\n";
    push @{$archname{$tape}{$pos}},$name;
    push @{$archsize{$tape}{$pos}},$size+0;
    push @{$archdirs{$tape}{$pos}},$dir;
    $tapesize{$tape}+=$size+0;
    $tapearch{$tape}++;
    $allsize+=$size;
    $allarch++;
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
  while(defined($line=<LSLR>)) {
    verbose 3,"LINE: $line";
    if($line =~ /^$tgtpath(\S*):\s*$/) {
      $subdir="$1";
      $subdir=~s:/+$::g;
      $subdir=~s:^/+::g;
      $dirname="$tgtpath/$subdir";
      verbose 2,"$subdir: scan directory $dirname\n";
      if(!defined($havedir{$subdir}) || !$havedir{$subdir}) {
        verbose 2,"$subdir: have dir\n";
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
    die "subdir undefined" unless defined $subdir;
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
  verbose 0,"$tgtpath: cannot list: $!.  Will assume target directory does not exist.\n";
}
close LSLR;

verbose 0,"Generate script (stdout)\n";

print $script "#!/bin/sh\n\nset -xu\n\n";

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
  print $script cmdify("cd $tgtpath ; mkdir -p ","",@mkdirme);
}

### Create staging and copying commands:
my ($files_left,$files_total)=(0,0);
my ($bytes_left,$bytes_total)=(0.0,0.0);
my $partial_copy=0;
foreach my $tape (sort{$a cmp $b} keys %archname) {
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

      verbose 2,"$srcpath/$name => $tgtpath/$name\n";

      $newname =~ s:/+:/:g;
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

  print $script "\n# TAPE $tape: $tapearch{$tape} archive(s), total of $tapesize{$tape} bytes\n";
  verbose 1,"TAPE $tape: remove ".(1+$#remove)." stage ".(1+$#stageme)." copy ".(1+$#copyme)." index ".(1+$#indexme)."\n";
  my @copydirs=keys %copydirs;
  my $ndirs=1+$#copydirs;
  print $script cmdify("cd $srcpath ; rm ","",@remove);
  my $dashS=" -S";
#  if($ndirs>1) {
#    verbose 1,"TAPE $tape: split across $ndirs directories; will have to stage.\n";
    print $script cmdify("cd $srcpath ; stage ","",@stageme);
    $dashS="";
#  }
  foreach my $dir (keys %copydirs) {
    my $srcdir=djoin($srcpath,$dir);
    my $tgtdir=djoin($tgtpath,$dir);
    print $script cmdify("cd $srcdir ; copy -p$dashS "," $tgtdir",@{$copydirs{$dir}});
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
verbose 0,sprintf("%3d%% of bytes copied; still have %0.3fTB of %0.3fTB to go\n",
                  $bytes_percent,$tb_left,$tb_total);
verbose 0,sprintf("%3d%% of files copied; still have %d of %d to go\n",
                  $files_percent,$files_left,$files_total);
if($partial_copy>1) {
  verbose 0,"WARNING: $partial_copy incomplete file copies detected.  They will be re-copied.\n";
} elsif($partial_copy==1) {
  verbose 0,"WARNING: $partial_copy incomplete file copy detected.  It will be re-copied.\n";
}
