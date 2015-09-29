#!/bin/env perl

use strict;
use warnings;
use File::Basename;

sub hsils {
  my $dir=$_[0];
  die "Perl broke: $dir" if $dir eq "1";
  my $hsils="hsi -P -q ls -l $dir/";
  $hsils=~s:/+:/:g;
  if(open(HSILS,"$hsils |")) {
    my @lines=<HSILS>;
    if(close(HSILS)) {
      return @lines;
    }
  }
  # We get here if we could not run hsi, or if we got a non-zero exit
  # status.
  warn "$dir: hsi mkdir -p\n";
  system("hsi -P -q mkdir -p $dir > /dev/null");
  open(HSILS,"$hsils |") or die "$hsils: cannot run after two tries: $!\n";
  my @lines=<HSILS>;
  close(HSILS) or die "$hsils: non-zero exit status after two tries: $!\n";
  return @lines;
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

if ($#ARGV!=2) {
    print "Format: make-hpss-copy-recursive.pl lsout srcdir tgtdir\n";
    print "  Will send to stdout a shell script to copy HTAR files recursively from srcdir to tgtdir\n";
    print "Arguments:\n";
    print "  lsout -- one of George V's hsi ls outputs\n";
    print "  srcdir -- HPSS source directory, must end with a /\n";
    print "  tgtdir -- HPSS target directory, must end with a /\n";
    print "NOTE: both directories must already exist.\n";
    print "Outputs:\n";
    print "  stdout stream -- the new shell script\n";
    print "  stderr stream -- list of files, and why they will or will not be\n";
    print "       copied or indexed.\n";
    exit 2;
}

my $lsout=$ARGV[0];
my $srcpath=$ARGV[1];
my $basename=basename $srcpath;
#warn "Basename is $basename";
my $tgtpath=$ARGV[2];
my $maxcmd=3000;

$srcpath=~s(/+$)()g;
$tgtpath=~s(/+$)()g;
$srcpath=~s(//+)(/)g;
$tgtpath=~s(//+)(/)g;

$srcpath=~s(^/NCEPDEV/)(/NCEPDEV-ReadOnly/)g;

open(LSOUT,"<$lsout") or die "$lsout: cannot read: $!";
my @lsout=<LSOUT>;
close LSOUT;
chomp @lsout;

### List known source files
my (%archname,%archsize,%tapesize,%tapearch,%archdirset,%archdirs);
my ($allsize,$allarch)=(0,0);
foreach my $lsout (@lsout) {
    next unless($lsout=~m(\Q$basename\E));
#/NCEPDEV-ReadOnly/hpssuser/g01/hurpara/jet-2013-hwrf-parallel/hhs/parallel-20130913-132005-hhs.tar
    unless($lsout=~m((?:TAPE|DISK)\s+(\d+)\s+.*\s\Q$srcpath\E(/(?:\S*/)?)([^/[:blank:]]+)\s.*Pos:\s*(\d+).*PV List: (.+?)\s*$)) {
        unless($lsout=~/\.idx\s+/) {
            warn "Bad line: $lsout\n";
        }
        next;
    }
    #next if $lsout=~/-temp-/;
    my ($size,$dir,$name,$pos,$tapebase)=($1,$2,$3,$4,$5);
    warn "$size - $dir - $name - $pos - $tapebase\n";
    my $tape=$tapebase;
    if($dir ne '/') {
      $name="$dir$name";
      $archdirset{$dir}=1;
    }
    $name=~s(^/+)()g;
    #warn "NAME NOW $name\n";
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

### Make a list of absolute source and target directories:
my @archdir='';
push @archdir,sort {$a cmp $b} keys %archdirset;
my (@archsrc,@archtgt);
foreach my $dir (@archdir) {
  push @archsrc,djoin($srcpath,$dir);
  push @archtgt,djoin($tgtpath,$dir);
}
if($#archsrc<0) {
  die "No source directories.\n";
}
### Make directory structure and list known target files:
my %tar=();
my %idx=();
for(my $id=0;$id<=$#archsrc;$id++) {
  my ($dir,$tgtdir,$srcdir)=($archdir[$id],$archtgt[$id],$archsrc[$id]);
  warn "Check $srcdir => $tgtdir\n";

  ### List known target files
  # my $hsils="hsi -P -q ls -l $tgtdir/";
  # $hsils=~s:/+:/:g;
  # unless(open(HSILS,"$hsils |")) {
  #   warn "$tgtdir: make directory\n";
  #   my $ret=system('hsi -P -q mkdir -p $tgtdir');
  #   if($ret != 0) {
  #     warn "Error status $ret from mkdir -p $tgtdir\n";
  #   }
  #   # Second failure to ls is fatal:
  #   open(HSILS,"$hsils |") or die "$hsils: cannot run after two tries: $!";
  # }
  # my @hsils=<HSILS>;
  my @hsils=hsils($tgtdir);
  my $nhsils=1+$#hsils;
  warn "... got back $nhsils lines.\n";
  close HSILS;
  foreach $_ (@hsils) {
    unless(m(^-r\S{8}\s+\S+\s+\S+\s+\S+\s+(\d+).*?([^/ \t]+)(\.tar\.idx|\.tar)?)) {
      warn "Skip line: $_";
      next;
    }
    #next if /-temp-/;
    my ($tsize,$tbase,$text)=($1,$2,$3);
    $text='' unless defined $text;
    my $name="$tbase$text";
    my $tgtfile=djoin($dir,"$tbase.tar");
    $tgtfile=~s:^/+::g;
    if($text eq ".tar.idx") {
      $idx{$tgtfile}=$tsize;
      warn "Index   $tgtfile size $tsize\n";
    } elsif($text eq ".tar") {
      $tar{$tgtfile}=$tsize;
      warn "Archive $tgtfile size $tsize\n";
    } else {
      $tar{$tgtfile}=$tsize;
      warn "Non-HTAR file $tgtfile size $tsize\n";
    }
  }
}

### Create staging and copying commands:
print "#!/bin/sh\n\nset -xu\n\n";
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

      $newname =~ s:/+:/:g;
      if ($newname =~ /\.tar.idx$/) {
        warn "$newname: skip index file\n";
        next;
      }
      if($size<=0) {
        warn "$name: empty; skip\n";
        next;
      }
      if (defined($tar{$name}) && $tar{$name}==$size) {
        if (defined($idx{$name}) && $idx{$name}>0) {
          warn "$name: done (tar size $tar{$name} index size $idx{$name}).\n";
        } elsif($name!~/\.tar$/) {
          warn "$name: not an HTAR archive, so not indexing.\n";
        } else {
          warn "$name: copied, but not indexed.  Will re-index.\n";
          if(defined($idx{$name})) {
            warn "$name: delete empty index file.\n";
            push @remove,"$newname.idx";
          }
          push @indexme,$newname;
        }
      } elsif(defined($tar{$name})) {
        warn "$name: partial copy ($tar{$name}!=$size); will re-copy.\n";
        push @remove,"$newname";
        if(defined($idx{$name}) && $newname=~/\.tar$/) {
          warn "$name: delete index file.\n";
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
        warn "$name: never copied; will copy file, and index it if it is an htar archive\n";
        if(defined($idx{$name}) && $newname=~/\.tar$/) {
          warn "$name: delete index file.\n";
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

  print "\n# TAPE $tape: $tapearch{$tape} archive(s), total of $tapesize{$tape} bytes\n";
  warn "TAPE $tape: remove ".(1+$#remove)." stage ".(1+$#stageme)." copy ".(1+$#copyme)." index ".(1+$#indexme)."\n";
  my @copydirs=keys %copydirs;
  my $ndirs=1+$#copydirs;
  print cmdify("cd $srcpath ; rm ","",@remove);
  my $dashS=" -S";
#  if($ndirs>1) {
#    warn "TAPE $tape: split across $ndirs directories; will have to stage.\n";
    print cmdify("cd $srcpath ; stage ","",@stageme);
    $dashS="";
#  }
  foreach my $dir (keys %copydirs) {
    my $srcdir=djoin($srcpath,$dir);
    my $tgtdir=djoin($tgtpath,$dir);
    print cmdify("cd $srcdir ; copy -p$dashS "," $tgtdir",@{$copydirs{$dir}});
  }
  foreach my $newname (@indexme) {
    print "htar -Xf $newname\n";
  }
}
