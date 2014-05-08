/* Enable GNU-specific and Linux-specific routines */
#define _GNU_SOURCE
#define _ATFILE_SOURCE

/* ENABLE_SPEED_STATS -- enables calculation of statistics based on
   processing speed (files/second).  (Command-line configurable, off
   by default.) */
#define ENABLE_SPEED_STATS

/* ENABLE_DELETION -- enables deletion of old files (command line
   configurable, off by default) */
#define ENABLE_DELETION

/* ENABLE_DISK_USAGE -- enables computation of disk usage (command
   line configurable, off by default) */
#define ENABLE_DISK_USAGE

/* ENABLE_CHECK_DUP -- enables checking of duplicate files such as
   hard links, or mv commands that were done during execution of this
   program.  This is command line configurable, but is on by
   default */
#define ENABLE_CHECK_DUP

#include <stdarg.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/acl.h>
#include <sys/stat.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <grp.h>
#include <time.h>

#ifdef ENABLE_DISK_USAGE
#include "disk_usage.h"
#endif

#ifdef ENABLE_CHECK_DUP
#include "check_dup.h"
#endif

#include "paranoia.h"
#include "basic_utils.h"

/* RECORD_STEP -- for features that do something every X files, such
   as throttling or speed statistics, this is the X */
#define RECORD_STEP 10000

/* INVALID_GID -- returned when code cannot determine the gid_t for a
   name */
#define INVALID_GID ((gid_t)(-1))

/* GLOBALS */
static acl_t acls[01000]; /* ACLs used for tag_rstprod commands */
/* pathbuf -- static path buffer used in walk_impl.  This is stored
   statically to avoid memory allocation overhead */
static char pathbuf[MAX_PATH_LEN_CHAR+517];
static gid_t required_gid=INVALID_GID; /* gid used for chgrp, when -g is given */
static gid_t rstprod_gid=INVALID_GID; /* gid of the rstprod group, when -r is given */
static size_t file_count=0; /* number of files seen */

/* Counts for filesystem modifications:
      setgid_count -- number of chmods done to set the setgid bit
      chgrp_count -- number of chowns done to change the group
      acl_count -- number of tag_rstprods done
      dir_count -- number of directories processed
      del_count -- number of unlinks done
*/
static size_t setgid_count=0, chgrp_count=0, acl_count=0, dir_count=0, del_count=0;

static double start_time;    /* start time in seconds since the epoch */
static size_t sleep_time=0;  /* number of seconds of sleeping done */

#ifdef ENABLE_DELETION
static int64_t delete_age=0; /* how old must a file be to be deleted */
static int delete_files=0;  /* should we delete files? */
static int delete_min_depth; /* minimum depth of files to delete */
#endif

#ifdef ENABLE_CHECK_DUP
static int check_dup=1; /* should we avoid processing a file twice?  (uses device/inode number) */
#endif

#ifdef ENABLE_SPEED_STATS
static int print_stats=0; /* do we calculate and print speed statistics */
#endif

#ifdef ENABLE_DISK_USAGE
static int disk_usage=0; /* do we calculate disk usage statistics */
#endif

/* if >MIN_THROTTLE, we throttle processing speed.  See throttle() for
   more info.  Must be <=100000 due to system time precision
   limits and limits of 64-bit doubles. */
static size_t throttle_rate=0; 

#define MIN_THROTTLE 10 /* throttle_rate<MIN_THROTTLE = do not throttle */

/* throttle -- throttle file processing rate if
   throttle_rate>MIN_THROTTLE.  This makes sure we do not process more
   than throttle_rate files per second.  It overthrottles typically,
   due to how it is written.

   The way it works is that it makes sure a file is not processed
   within 1.0/throttle_rate seconds after the previous file.  Some
   files will take longer than that to process, hence fewer files will
   be processed per second than throttle_rate, especially if
   filesystem modifications are done.
*/
void throttle() {
  static double lasttime=0,minspan=0;
  static int inited=0;
  if(throttle_rate<MIN_THROTTLE)
    /* Assume less than 10 files per second means "don't throttle" */
    return;
  if(!inited) {
    /* We get here only once, the first time we hit a file.
       Initialize the throttling variables. */
    inited=1;
    lasttime=fulltime();
    if(throttle_rate>100000) {
      warn("Changing to maximum allowed throttle of 100000 files per second.\n");
      throttle_rate=100000;
    }
    /* Calculate the throttling timespan: the minimum amount of time
       between files */
    minspan=1/((double)throttle_rate);
  } else {
    double now,span;
    int tries;
    for(tries=0;tries<10;tries++) {
      now=fulltime();
      span=now-lasttime; /* span = how long it took to process the last file */
      if(span<0)
        /* Error: the last file processed was processed in the future.
           Do not throttle. */
        break;
      if(span>minspan)
        /* The file took longer to process than the throttling
           timespan, so we have no throttling to do. */
        break;
      else {
        /* The file took less time to process than the throttling
           timespan so we need to throttle. */
        unsigned int sleeptime=(minspan-span)*1e6;
        sleeptime = (sleeptime>1000000) ? 1000000 : sleeptime;
        sleeptime = (sleeptime<1) ? 1 : sleeptime;
        usleep(sleeptime);
      }
    }
    /* Record the current time so we can check the time it takes to
       process the next file. */
    lasttime=fulltime();
  }
}

/* dir_enter: called every time a directory is entered.  Intended to
   be used for disk space accounting. */
static void dir_enter(const char *dirname,const struct stat *dirstat) {
#ifdef ENABLE_DISK_USAGE
  us_dir_enter(dirname,dirstat);
#endif /* ENABLE_DISK_USAGE */
}
/* file_found: called for each filesystem object seen.  Intended to be
   used for disk space accounting.  This is where we trigger any
   features that must be done per file for non-deleted files. */
static void file_found(const char *filename,const struct stat *filestat) {
  static double last_time=0;
  static size_t last_count=0;
  static int inited=0;

  file_count++;

  /* If we're enabling disk usage statistics, call the disk usage
     information storage function */
#ifdef ENABLE_DISK_USAGE
  us_file_found(filename,filestat);
#endif /* ENABLE_DISK_USAGE */

  if(file_count && file_count%RECORD_STEP == 0) {
    double now=fulltime();
    /* Handle speed statistics, if we're doing that */
#ifdef ENABLE_SPEED_STATS
    if(print_stats) {
      if(inited)
        printf("Did %llu files (%llu changes) in %.3f sec (%.2f/sec avg, %.2f/sec recently)...\n",
               (unsigned long long)file_count,
               (unsigned long long)(setgid_count+chgrp_count+acl_count+del_count),
               now-start_time,file_count/(now-start_time-sleep_time),
               (file_count-last_count)/(now-last_time));
      else
        printf("Did %llu files (%llu changes) in %.3f sec (%.2f/sec avg)...\n",
               (unsigned long long)file_count,
               (unsigned long long)(setgid_count+chgrp_count+acl_count+del_count),
               now-start_time,file_count/(now-start_time-sleep_time));
    }
#endif /* ENABLE_SPEED_STATS */

    /* Next, if we're throttling, include code to sleep if the speed
       is very, very slow.  This is done under the assumption that the
       metadata server is running into serious issues when the file
       speed is getting unreasonably slow. */
    if(inited) {
      double recent_rate=(file_count-last_count)/(now-last_time);
      if(recent_rate<100.0 && throttle_rate>300) {
        printf("WARNING: rate dropped below 100/second.  Sleeping 120 seconds.\n");
        sleep(120);
        sleep_time+=120;
      } else if(recent_rate<250.0 && throttle_rate>700) {
        printf("WARNING: rate dropped below 250/second.  Sleeping 60 seconds.\n");
        sleep(60);
        sleep_time+=60;
      } else if(recent_rate<500.0 && throttle_rate>1000) {
        printf("WARNING: rate dropped below 500/second.  Sleeping 30 seconds.\n");
        sleep(30);
        sleep_time+=30;
      } else if(recent_rate<1000.0 && throttle_rate>2000) {
        printf("WARNING: rate dropped below 1000/second.  Sleeping 15 seconds.\n");
        sleep(15);
        sleep_time+=15;
      }
    }

    /* Now record the current time so we will know how long it has
       been since the last call */
    last_time=now;
    last_count=file_count;
    inited=1;
  }
}
/* dir_leave: called every time a directory is left.  Intended to be
   used for disk space accounting. */
static void dir_leave(const char *dirname,const struct stat *dirstat) {
#ifdef ENABLE_DISK_USAGE
  us_dir_leave(dirname,dirstat);
#endif /* ENABLE_DISK_USAGE */
}

/* init_acls: create an array of access control list objects, one per
   possible mode (000 through 777).  We set the rstprod group access
   to the group access portion of the mode, and the user access to the
   user access portion of the mode.  Other (world) access is
   ignored. */
void init_acls(const char *rstprod) {
  const char *format="u::%c%c%c,g::---,g:%s:%c%c%c,o::---,m::rwx";
  size_t slen;
  char *buffer;
  mode_t mode;

  slen=group_length(rstprod,1) + strlen(format) + 20;

  if(!(buffer=(char *)malloc(slen)))
    fail("Cannot allocate %llu bytes: %s\n",
         (unsigned long long)slen,strerror(errno));

  for(mode=00000;mode<01000;mode+=010) {
    assert((mode&0770)<01000); // bounds check
    snprintf(buffer,slen-1,format,
             (mode&0400)?'r':'-',
             (mode&0200)?'w':'-',
             (mode&0100)?'x':'-',
             rstprod,
             (mode&0040)?'r':'-',
             (mode&0020)?'w':'-',
             (mode&0010)?'x':'-');
    if( !(acls[mode]=acl_from_text(buffer)))
      fail("Cannot create an ACL: %s\n",strerror(errno));
  }
  free(buffer);
}

/* tag_rstprod: tags a file as rstprod via ACLs using the method
   described above in init_acls */
int tag_rstprod(const char *filename,mode_t mode) {
  size_t index=mode&0770;
  int ret1=0,ret2;
  assert(index<01000); // bounds check
  path_length(filename,1);


  if(S_ISDIR(mode))
    /* We need two callls to acl_set_file for directories: one for the
       ACL, and one for the default ACL.  We set the Default ACL here: */
    if((ret1=acl_set_file(filename,ACL_TYPE_DEFAULT,acls[index])))
      warn("%s: cannot set default ACL: %s\n",filename,strerror(errno));

  /* Set the regular, non-default ACL here: */
  if((ret2=acl_set_file(filename,ACL_TYPE_ACCESS,acls[index])))
    warn("%s: cannot set ACL: %s\n",filename,strerror(errno));

  return ret1 || ret2;
}

/* gid_for: returns the group ID for the specified group name. */
gid_t gid_for(const char *groupname) {
  struct group *g;
  group_length(groupname,1);

  if(!(g=getgrnam(groupname)))
    fail("%s: cannot find this group: %s\n",groupname,strerror(errno));
  return g->gr_gid;
}

/* walk_impl: this routine does the actual walking of the directory tree
   pathlen -- length of the pathbuf (file/dir path) upon entry to this function
   d -- directory object from opendir(pathbuf)
   depth -- recursion depth, starting at 1 for the top-level directory
   dirstat -- struct stat for this directory
   emptied -- *emptied is set to 1 if everything in the directory is deleted,
       set to 0 otherwise */
void walk_impl(size_t pathlen,DIR *d,size_t depth,
               const struct stat *dirstat,int *emptied) {
  struct dirent *dent;
  DIR *subdir_opened;
  size_t basenamelen,newpathlen,oldpathlen;
  struct stat statbuf;
  int rstokay,statted,deleted;
  int use_lustre_stat=get_use_lustre_stat(),duplicate=0;

#ifdef ENABLE_DELETION
  int can_delete;
  size_t files_seen=0, deletions=0;
  int subdir_emptied;
#endif

  /* Check assumptions: */
  assert(depth<=MAX_PATH_DEPTH);
  assert(d);
  assert(pathlen>0);
  assert(pathlen<MAX_PATH_LEN_CHAR);

  /* Store the length of the path string for this directory: */
  oldpathlen=pathlen;

  /* Indicate that we're entering this directory */
  dir_enter(pathbuf,dirstat);

  debugn(VERB_DEBUG_HIGH,"%s: entering directory\n",pathbuf);
  dir_count++;

  /* Loop over all files in this directory */
  while( (dent=readdir(d)) ) {
    /* Make sure the file basename is within the allowed limits */
    basenamelen=basename_length(dent->d_name,0);
    if(basenamelen==BAD_LEN) {
      warn("%s%*s...: skipping: file basename is too long",pathbuf,basename,MAX_BASENAME_LEN);
      us_filename_too_long(pathbuf,dent->d_name,dirstat);
      continue;
    }

    /* Skip . and .. */
    if(!strcmp(dent->d_name,".") || !strcmp(dent->d_name,".."))
      continue; /* Skip . and .. */

    /* Initialize deletion variables if we're allowing file deletion
       at compilation time: */
#ifdef ENABLE_DELETION
    can_delete=1;
    files_seen++;
#endif

    /* Make sure the path, after appending the file basename, is
       within allowed limits: */
    if(basenamelen+pathlen>MAX_PATH_LEN_CHAR) {
      warn("%s%*s...: skipping: path length is too long",pathbuf,basename,basenamelen);
      us_path_too_long(pathbuf,dent->d_name,dirstat);
      continue;
    }

    /* Stat the file: */
    statted=0;
    if(use_lustre_stat) {
      if((lustre_lstatfd(d,dent->d_name,basenamelen,&statbuf)))
        warn("%s%s: cannot stat using lustre stat: %s\n",pathbuf,dent->d_name,strerror(errno));
      else
        statted=1;
    } else if((fstatat(dirfd(d),dent->d_name,&statbuf,AT_SYMLINK_NOFOLLOW)))
        warn("%s%s: cannot stat: %s\n",pathbuf,dent->d_name,strerror(errno));
    else
      statted=1;

    if(!statted)
        continue; /* stat failed on this file */

    /* Append the file basename to the pathbuf: */
    newpathlen=pathlen+basenamelen;
    memcpy(pathbuf+pathlen,dent->d_name,basenamelen);
    pathbuf[newpathlen]='\0';

    debugn(VERB_DEBUG_HIGH,"%s: process file\n",pathbuf);

    /* Check for duplicate device/inode if requested: */
#ifdef ENABLE_CHECK_DUP
    if((duplicate=hit_file(statbuf.st_dev,statbuf.st_ino)))
      debug("%s: already processed.  Hard link?\n",
            pathbuf);
#endif

    /* Indicate that the file has not been deleted: */
    deleted=0;

    /* recurse into this subdirectory if allowed and possible */
    if(S_ISDIR(statbuf.st_mode)) {
#ifdef ENABLE_DELETION
      can_delete=0;
      subdir_emptied=0;
#endif
      if(!duplicate) {
        if(depth<MAX_PATH_DEPTH) {
          if((subdir_opened=opendir(pathbuf))) {
            /* We can recurse into this directory. */

            /* Append a / to the path */
            pathbuf[newpathlen]='/';
            pathbuf[newpathlen+1]='\0';

            /* Recurse: */
            walk_impl(newpathlen+1,subdir_opened,depth+1,&statbuf,&subdir_emptied);

            /* Close the directory: */
            closedir(subdir_opened);

            /* Remove the / from the path */
            pathbuf[newpathlen]='\0';

            /* Indicate whether we can delete the directory.  */
#ifdef ENABLE_DELETION
            can_delete=subdir_emptied;
            debugn(VERB_DEBUG_HIGH,"%s: setting can_delete=subdir_emptied=%d\n",pathbuf,subdir_emptied);
#endif
          } else {
            warn("%s: opendir failed: %s\n",pathbuf,strerror(errno));
            us_dir_unopenable(pathbuf,&statbuf);
          }
        } else {
          warn("%s: owned by %llu is beyond maximum allowed directory depth of %llu\n",
               pathbuf,(unsigned long long)statbuf.st_uid,MAX_PATH_DEPTH);
          us_dir_too_deep(pathbuf,&statbuf);
        }
      } else
        debug("%s: duplicate directory, not recursing\n",pathbuf);
    }
#ifdef ENABLE_DELETION
    else
      /* This is not a directory.  That means, so far, we are allowed to delete it. */
      can_delete=1;

    /* Can we delete this file? */
    if(delete_files && can_delete && (int64_t)depth>=(int64_t)delete_min_depth) {
      /* Yes, so far.  The only check left is the age. */
      time_t now=time(NULL);
      int64_t m_age=((int64_t)now)-((int64_t)statbuf.st_mtime);
      //int64_t c_age=((int64_t)now)-((int64_t)statbuf.st_ctime);
      int64_t age;

      /* Age check: 
         links: age is the lesser of access age and modify age
         others: age is the modification age
      */

      if(S_ISDIR(statbuf.st_mode))
        age=m_age; //(m_age<c_age) ? m_age : c_age;
      else
        age=m_age;
      if(age>=delete_age) {
        /* The file can be deleted. */
        debug("%s: age %llds >= %llds; delete file\n",pathbuf,age,delete_age);
        del_count++;
        if(unlinkat(dirfd(d),dent->d_name,
                    (S_ISDIR(statbuf.st_mode)) ? AT_REMOVEDIR : 0))
          warn("%s: unlinkat failed: %s\n",pathbuf,strerror(errno));
        else {
          /* Unlink succeeded.  This file has been deleted. */
          deletions++;
          deleted=1;
        }
      } else {
        debugn(VERB_DEBUG_HIGH,"%s: age %llds < %llds; not deleting file\n",pathbuf,age,delete_age);
      }
    } else if(delete_files) {
      /* We are not allowed to delete this file.  If debug level is
         very high (-v -v) then print out a reason why */
      if((int64_t)depth<(int64_t)delete_min_depth)
        debugn(VERB_DEBUG_HIGH,"%s: cannot delete: not past min depth (%d<%d)\n",
               pathbuf,depth,delete_min_depth);
      else if(!subdir_emptied)
        debugn(VERB_DEBUG_HIGH,"%s: cannot delete: subdirectory is not empty\n",pathbuf);
      else if(S_ISDIR(statbuf.st_mode) && duplicate)
        debugn(VERB_DEBUG_HIGH,"%s: cannot delete: did not recurse into duplicate directory\n",pathbuf);
      else
        debugn(VERB_DEBUG_HIGH,"%s: cannot delete or deletion is disabled\n",pathbuf);
    }
#endif

    if(!duplicate && !deleted) {
      /* We did not delete this directory, and it is not a duplicate,
         so let's change its group ids, setgid bit and rstprod tagging
         if relevant */

      /* Should we turn on the setgid bit? */
      if(S_ISDIR(statbuf.st_mode) && required_gid!=(gid_t)-1 && !(statbuf.st_mode&S_ISGID)) {
        debug("%s: set gid\n",pathbuf);
        setgid_count++;
        if(fchmodat(dirfd(d),dent->d_name,(statbuf.st_mode&0777)|S_ISGID,0))
          warn("%s: cannot add setgid bit: %s\n",pathbuf,strerror(errno));
      }
      
      /* Should we tag the directory as rstprod via ACLs? */
      rstokay=1; /* set to 1 if rstprod tagging worked */
      if(rstprod_gid!=(gid_t)-1 && statbuf.st_gid==rstprod_gid && !S_ISLNK(statbuf.st_mode)) {
        debug("%s: tag rstprod\n",pathbuf);
        acl_count++;
        rstokay=!tag_rstprod(pathbuf,statbuf.st_mode);
      }

      /* Should we chgrp the file/dir? */
      if(rstokay && required_gid!=(gid_t)-1 && statbuf.st_gid!=required_gid) {
        debug("%s: chgrp\n",pathbuf);
        chgrp_count++;
        if(fchownat(dirfd(d),dent->d_name,(uid_t)-1,required_gid,AT_SYMLINK_NOFOLLOW))
          warn("%s: cannot chgrp: %s\n",pathbuf,strerror(errno));
      }
    }

    /* Throttle file accessing speed if requested: */
    throttle();

    if(deleted)
      /* File was deleted, so call the us_file_deleted to record usage information: */
      us_file_deleted(pathbuf,&statbuf);
    else if(!duplicate)
      /* The file was not deleted, and is not a duplicate, so call all
         relevant per-file routines. */
      file_found(pathbuf,&statbuf);

    /* Clip the pathbuf so it only contains the directory path */
    pathbuf[pathlen]='\0';
  }

  /* indicate that we're leaving this directory */
  dir_leave(pathbuf,dirstat);
  debug("%s: leaving directory\n",pathbuf);

  /* If deletions are enabled, indicate whether this directory's
     entire contents have been deleted. */
#ifdef ENABLE_DELETION
  if(delete_files) {
    if(deletions>=files_seen) {
      debug("%s: deleted all files\n",pathbuf);
      *emptied=1;
    } else {
      debug("%s: %llu of %llu files not deleted\n",pathbuf,deletions,files_seen);
      *emptied=0;
    }
  }
#else
  *emptied=0;
#endif
}

/* walk: recurses through a directory tree, processing all files.  The
   directory is statted using the selected stat method to ensure that
   the device and inode numbers match what is seen internally in
   walk_impl. */
void walk(const char *dirname) {
  DIR *d;
  size_t len=path_length(dirname,1);
  struct stat statbuf;
  int emptied;
  memcpy(pathbuf,dirname,len);
  pathbuf[len]='/';
  pathbuf[len+1]='\0';
  if(similar_lstat(dirname,&statbuf)) {
    warn("%s: cannot stat: %s\n",dirname,strerror(errno));
    return;
  }
  if(!(d=opendir(dirname))) {
    warn("%s: cannot open directory: %s\n",dirname,strerror(errno));
    return;
  }
  walk_impl(len+1,d,1,&statbuf,&emptied);
  closedir(d);
}

/* usage: print a usage message and exit.
     exename -- name of this executable, gotten from argv[0]
     message -- if NULL, everything is sent to stdout, and 
        usage calls exit(0) 
      if non-NULL, everything goes to stderr.  Message is sent
      to stderr, and usage callse exit(1).
This routine does not return.
*/
void usage(const char *exename,const char *message) {
  fprintf( ( (message==NULL) ? stdout : stderr ),
           "Syntax: %s [-g group] [-r rstprod]\n"
           "\n"
           "  -g group -- chgrp everything to this group, and\n"
           "              set the setgid bit on directories\n"
           "  -r rstprod -- all rstprod group files and directories\n"
           "              are given ACLs to allow rstprod group access,\n"
           "              owner access, but no other access.\n"
           "  -v -- be verbose.\n"
           "  -q -- silent mode; print nothing other than fatal\n"
           "        errors"
           "  -L -- disable use of Lustre stat.  Don't use this except for\n"
           "        speed tests; it will slow down the program by a lot.\n"
           "  -l -- enable use of Lustre stat.  This is on by default.\n"
#ifdef ENABLE_SPEED_STATS
           " and final stats (if -s is given).\n"
           "  -s -- print speed statistics.\n"
#endif /* ENABLE_SPEED_STATS */
#ifdef ENABLE_DISK_USAGE
           "  -u /path/to/directory -- keep track of disk usage\n"
           "        within this directory.  You may specify -u any\n"
           "        number of times.\n"
           "  -b bytesize -- set the size of a file that is considered\n"
           "        \"big\" for the purposes of disk usage accounting\n"
           "        (meaningless without -u).\n"
           "  -x /prefix/for/xml/reports -- prefix to prepend to filenames\n"
           "        of files that will contain XML reports of usage stats.\n"
           "        This option is meaningless without -u\n"
           "  -F -- also generate a list of all files and some attributes\n"
#endif /* ENABLE_DISK_USAGE */
#ifdef ENABLE_DELETION
           "  -d days -- delete everything older than this number of days.\n"
           "        Fractions are okay.\n"
           "  -D mindepth -- do not delete anything less than this depth\n"
           "        within the file tree.\n"
#endif
#ifdef ENABLE_CHECK_DUP
           "  -n -- disable checking for duplicate files (hard links).  This\n"
           "        will save a significant amount of memory: ~16B/file\n"
#endif
           "  -t N -- ensure that less than N files will be processed\n"
           "        per second\n"
           "  -h -- print this help message and exit.\n",
           exename);
  if(message)
    fprintf(stderr,message);
  exit(message ? 1 : 0);
  fail("Exit did not exit: %s\n",strerror(errno));
}

/**********************************************************************/
/**  MAIN PROGRAM  ****************************************************/
/**********************************************************************/


int main(int argc,char **argv) {
  int opt,arg;
  double end;

  /* Calculate argument list to send to getopt */
  const char *arglist=
#ifdef ENABLE_SPEED_STATS
    "s"
#endif
#ifdef ENABLE_DISK_USAGE
    "u:x:b:F"
#endif
#ifdef ENABLE_DELETION
    "d:D:"
#endif
    "g:qt:vlr:hL";
  const char *rstprod=NULL,*xml_pre="./";

  setlinebuf(stdout);

  /* Loop over all dash options, processing them via getopt */
  while((opt=getopt(argc,argv,arglist))!=-1) {
    switch(opt) {
    case 'g': required_gid=gid_for(optarg); break;
    case 'r':
      rstprod_gid=gid_for(optarg);
      rstprod=optarg;
      break;
    case 'h': usage(argv[0],NULL); break;
    case 'l': set_use_lustre_stat(1); break;
    case 'L': set_use_lustre_stat(0); break;
    case 'v': increment_verbosity(); break;
    case 'q': set_verbosity(VERB_FATAL); break;
#ifdef ENABLE_SPEED_STATS
    case 's': print_stats=1; break;
#endif /* ENABLE_SPEED_STATS */
#ifdef ENABLE_DISK_USAGE
    case 'b': us_set_big_file_size((size_t)atoll(optarg)); break;
    case 'u': us_add_dir(optarg); disk_usage=1; break;
    case 'x': xml_pre=optarg; break;
    case 'F': us_list_all_files(1); break;
#endif /* ENABLE_DISK_USAGE */
#ifdef ENABLE_DELETION
    case 'd':
      delete_age=atof(optarg)*24*3600;
      if(delete_age<=0)
        delete_age=1;
      delete_files=1;
      break;
    case 'D': 
      delete_min_depth=atoll(optarg);
      if(delete_min_depth<1)
        delete_min_depth=1;
      break;
#endif
#ifdef ENABLE_CHECK_DUP
    case 'n': check_dup=0; break;
#endif
    case 't': throttle_rate=atoi(optarg); break;

    default:  usage(argv[0],"Invalid argument given.\n");
    }
  }

  /* Check arguments */
  if(optind>=argc)
    usage(argv[0],"Specify at least one directory.\n");

  if(rstprod_gid!=INVALID_GID)
    init_acls(rstprod);

#ifdef ENABLE_DISK_USAGE
  if(disk_usage)
    us_start_reports(xml_pre,start_time,MAX_PATH_DEPTH);
#endif /* ENABLE_DISK_USAGE */


  /* Record walking start time */
  start_time=fulltime();

  /* Loop over all given directories */
  for(arg=optind;arg<argc;arg++)
    walk(argv[arg]);

  /* Record walking end time */
  end=fulltime();

  /* Generate XML usage reports */
#ifdef ENABLE_DISK_USAGE
  if(disk_usage)
    us_generate_reports(xml_pre,start_time,end,MAX_PATH_DEPTH);
#endif /* ENABLE_DISK_USAGE */

  /* Output final speed statistics, if requested */
#ifdef ENABLE_SPEED_STATS
  if(print_stats) {
    printf("Processed %llu files in %f seconds, sleeping %llu seconds (%f files per second)\n",
           (unsigned long long)file_count,end-start_time,(unsigned long long)sleep_time,
           file_count/(end-start_time-sleep_time));
    printf("  setgid         ... %llu times\n"
           "  chgrp          ... %llu times\n"
           "  tagged rstprod ... %llu times\n"
           "  entered dirs   ... %llu times\n"
           "  deleted things ... %llu times\n",
           (unsigned long long)setgid_count,
           (unsigned long long)chgrp_count,
           (unsigned long long)acl_count,
           (unsigned long long)dir_count,
           (unsigned long long)del_count);
  }
#endif
  return 0;
}
