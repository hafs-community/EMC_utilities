#define _GNU_SOURCE
#define _ATFILE_SOURCE

#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/time.h>
#include <assert.h>
#include <errno.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <lustre/lustre_user.h>
#include <fcntl.h>

#include "paranoia.h"
#include "basic_utils.h"

/**********************************************************************/
/** These types come from /usr/include/lustre/lustre_idl.h which
    cannot be included directly outside of the kernel.  These are
    needed to get the size of the block of memory to send to ioctl
    when directly calling lustre's lstat implementation. **/

#define lov_ost_data lov_ost_data_v1
struct lov_ost_data_v1 {          /* per-stripe data structure (little-endian)*/
  __u64 l_object_id;        /* OST object ID */
  __u64 l_object_seq;       /* OST object seq number */
  __u32 l_ost_gen;          /* generation of this l_ost_idx */
  __u32 l_ost_idx;          /* OST index in LOV (lov_tgt_desc->tgts) */
};

#define lov_mds_md lov_mds_md_v1
struct lov_mds_md_v1 {            /* LOV EA mds/wire data (little-endian) */
  __u32 lmm_magic;          /* magic number = LOV_MAGIC_V1 */
  __u32 lmm_pattern;        /* LOV_PATTERN_RAID0, LOV_PATTERN_RAID1 */
  __u64 lmm_object_id;      /* LOV object ID */
  __u64 lmm_object_seq;     /* LOV object seq number */
  __u32 lmm_stripe_size;    /* size of stripe in bytes */
  __u32 lmm_stripe_count;   /* num stripes in use for this object */
  struct lov_ost_data_v1 lmm_objects[0]; /* per-stripe data */
};

struct lov_mds_md_v3 {            /* LOV EA mds/wire data (little-endian) */
  __u32 lmm_magic;          /* magic number = LOV_MAGIC_V3 */
  __u32 lmm_pattern;        /* LOV_PATTERN_RAID0, LOV_PATTERN_RAID1 */
  __u64 lmm_object_id;      /* LOV object ID */
  __u64 lmm_object_seq;     /* LOV object seq number */
  __u32 lmm_stripe_size;    /* size of stripe in bytes */
  __u32 lmm_stripe_count;   /* num stripes in use for this object */
  char  lmm_pool_name[LOV_MAXPOOLNAME]; /* must be 32bit aligned */
  struct lov_ost_data_v1 lmm_objects[0]; /* per-stripe data */
};
/**********************************************************************/

/* verbosity -- what level of verbosity did the user request?  Used to
   decide which warning and debug messages to print */
static int verbosity=VERB_WARN;

/* use_lustre_stat -- should we use the lustre implementation of
   lstat?  If false, we use fstatfd in lstat mode instead. */
static int use_lustre_stat=1;

/* set_use_lustre_stat/get_use_lustre_stat -- use to get or modify
   use_lustre_stat outside of this object file */
void set_use_lustre_stat(int yesno) {
  use_lustre_stat=yesno;
}
int get_use_lustre_stat(void) {
  return use_lustre_stat;
}

/* increment_verbosity/set_verbosity -- use these to modify verbosity
   outside of this object file */
void increment_verbosity() {
  verbosity++;
}
void set_verbosity(int x) {
  verbosity=x;
}

/* fail/warn/debug/debugn -- print messages to stderr if the verbosity
   has reached a certain level.  Fail always prints and then exits the
   program. */
void fail(const char *format,...) {
  va_list ap;
  va_start(ap,format);
  vfprintf(stderr,format,ap);
  va_end(ap);
  exit(2);
  abort(); /* should never reach here */
}

void warn(const char *format,...) {
  va_list ap;
  if(verbosity>=VERB_WARN) {
    va_start(ap,format);
    vfprintf(stderr,format,ap);
    va_end(ap);
  }
}

void debug(const char *format,...) {
  va_list ap;
  if(verbosity>=VERB_DEBUG) {
    va_start(ap,format);
    vfprintf(stderr,format,ap);
    va_end(ap);
  }
}

void debugn(int level,const char *format,...) {
  va_list ap;
  if(verbosity>=level) {
    va_start(ap,format);
    vfprintf(stderr,format,ap);
    va_end(ap);
  }
}

/* fulltime -- get the unix epoch time including fractions.  Should
   have precision of 1e-6 seconds. */
double fulltime() {
  struct timeval tv;
  gettimeofday(&tv,NULL);
  return tv.tv_sec + tv.tv_usec*1e-6;
}

/* max_fileinfo_buffer_size -- Calculates the size of the buffer
   needed to call ioctl with request number IOC_MDC_GETFILEINFO, the
   request number for Lustre's lstat implementation.  This calculation
   may need to be changed for new Lustre versions.  For that reason,
   we add a huge amount (add_just_in_case) to the estimated size
   required, just in case. */
size_t max_fileinfo_buffer_size(void) {
  /* WARNING: MUST BE CHANGED FOR NEW LUSTRE VERSIONS 
   We use add_just_in_case as a safeguard against that. */

  /* This calculation comes from lov_mds_md_size in
     lustre/include/obd_lov.h inside the Lustre repository, except for
     the MAX_LOV_UUID_COUNT which is the maximum number of striping
     entries, which comes from lustre/utils/liblustreapi.c in the
     Lustre repository. */

  const size_t basesize=sizeof(lstat_t) + sizeof(struct lov_user_md_v3) + sizeof(struct lov_mds_md_v3);
  const size_t per_entry=sizeof(struct lov_ost_data_v1);
  const size_t MAX_LOV_UUID_COUNT=1000;
  const size_t add_just_in_case=500000; /* just in case the numbers are wrong, add padding */

  const size_t returnvalue= basesize + MAX_LOV_UUID_COUNT*per_entry + add_just_in_case;

  return returnvalue;
}

/* lustre_lstatfd -- uses lustre's lstat implementation as a
   replacement for fstatfd.  Arguments:

    dir -- the directory in which the file resides
    path -- the filename within that directory.
    pathlen -- length of the path in chars.  Optional.  Set to zero and it
              will be calculated for you
    sb -- stat structure to contain the output

    Returns 0 on success, non-zero on failure.  Memory allocation failures 
    will cause fail() to be called.
*/
int lustre_lstatfd(DIR *dir,const char *path,size_t pathlen,struct stat *sb) {
  static int allocated=0;
  static struct lov_user_mds_data *buf;
  static size_t bufsize=0;
  int ret;
  assert(sb);
  if(!pathlen)
    pathlen=path_length(path,1);
  if(!allocated) {
    allocated=1;
    bufsize=max_fileinfo_buffer_size();
    assert(bufsize);
    if(!(buf=malloc(bufsize)))
      fail("Cannot allocate %llu bytes: %s\n",
           (unsigned long long)bufsize,strerror(errno));
  }
  if(pathlen>bufsize)
    fail("Attempted to open a file with a name longer than allowed by lustre's stat (%llu>%llu)\n",
         (unsigned long long)pathlen+1,(unsigned long long)bufsize);

  memcpy(buf,path,pathlen+1);
  ret=ioctl(dirfd(dir), IOC_MDC_GETFILEINFO, (void*)buf);
  memcpy(sb,&(buf->lmd_st),sizeof(struct stat));
  return ret;
}

/* Splits a path into directory and basename components.  Uses static
   storage. */
void path_split(const char *full,char **dirname,char **basename) {
  typedef unsigned long long ull;
  size_t len=path_length(full,1);
  static int inited=0;
  static char *dup=NULL;
  static size_t alloclen=0;
  int prevslash;
  const char *last=full+len-1,*before_basename,*from;
  char *to;

  assert(len+1000>len);

  //fprintf(stderr,"%s: before: len=%llu alloclen=%llu\n",full,(ull)len,(ull)alloclen);

  if(!inited) {
    //fprintf(stderr,"%s: init: len=%llu alloclen=%llu\n",full,(ull)len,(ull)alloclen);
    dup=(char*)malloc(len+10);
    if(!dup)
      fail("%s: cannot allocate %llu bytes: %s\n",
           full,(unsigned long long)(len+10),strerror(errno));
    assert(dup);
    alloclen=len+10;
    inited=1;
  } else {
    if(len+10>alloclen) {
      //fprintf(stderr,"%s: realloc: len=%llu alloclen=%llu\n",full,(ull)len,(ull)alloclen);
      dup=realloc(dup,len+10);
      if(!dup)
        fail("%s: cannot allocate %llu bytes: %s\n",
             full,(unsigned long long)(len+10),strerror(errno));
      assert(dup);
      alloclen=len+10;
    } else {
      //fprintf(stderr,"%s: okay: len=%llu alloclen=%llu (%llu %d)\n",full,(ull)len,(ull)alloclen,(ull)(len+10),(int)(len+10>alloclen));
    }
  }

  //fprintf(stderr,"%s: after: len=%llu alloclen=%llu\n",full,(ull)len,(ull)alloclen);

  assert(alloclen>=len+10);
  assert(dup-10<dup);
  assert(full-10<full);  
  assert(dup);
  assert(full);
  assert(basename);
  assert(dirname);

  if(*full=='\0') /* Detect empty strings */
    goto rootdir;

  assert(dup+len+10>dup);
  assert(full+len>full);

  /* Skip trailing / chars */
  while(last>=full && *last=='/') last--;
  
  if(last<full) /* Detect a string of only / */
    goto rootdir;
  
  /* Find the next / */
  before_basename=last-1;
  while(before_basename>=full && *before_basename!='/') before_basename--;

  if(before_basename<full) /* Detect a string with no path components */
    goto no_path;

  /* This string has path and basename components, so split them. */

  /* Copy the directory path and remove duplicate / chars */
  to=dup; from=full; prevslash=0;
  *dirname=dup;
  assert(from<=before_basename);
  while(from<=before_basename) {
    if(prevslash && *from!='/') {
      //fprintf(stderr,"%llu: prevslash from=%c assign\n",(ull)(to-dup),*from);
      *to=*from;
      prevslash=0;
      assert(to<dup+alloclen);
      to++;
    } else if(!prevslash) {
      if(*from=='/') {
        //fprintf(stderr,"%llu: not prevslash, but *from==%c\n", (ull)(to-dup),*from);
        prevslash=1;
      } else {
        //fprintf(stderr,"%llu: not prevslash and *from==%c\n", (ull)(to-dup),*from);
      }
      assert(to<dup+alloclen);
      *to=*from;
      to++;
    }
    from++;
  }
  if(to>dup+1) {
    assert(to<dup+alloclen);
    assert(to>dup);
    to[-1]='\0';
  } else { /* Path is / */
    *to='\0';
    to++;
  }
  *basename=to;

  /* Copy in the basename */
  assert(to+(last-before_basename)>dup);
  assert(to+(last-before_basename)<dup+alloclen);
  memcpy(to,before_basename+1,last-before_basename);
  to[last-before_basename]='\0';
  return;

 rootdir: 
  /* Handle root directory (or empty string) */
  assert(alloclen>2);
  dup[0]='/'; dup[1]='\0';
  *basename=dup; *dirname=dup;
  return;

 no_path:
  /* Handle a string with no path components.  Path is "." */
  assert(alloclen>2);
  dup[0]='.'; dup[1]='\0';
  assert(dup+2+(last-before_basename)+1<dup+alloclen);
  memcpy(dup+2,before_basename+1,last-before_basename);
  dup[2+last-before_basename]='\0';
  *dirname=dup;
  *basename=*dirname+2;  
  return;
}

/* parent_statfd: exactly the same as lustre_fstatfd, but uses the
   fstatfd in lstat mode. */
int parent_fstatfd(DIR *dir,const char *path,size_t pathlen,struct stat *sb) {
  int ret=0;
  DIR *mydir=NULL;
  char *bn,*dn;
  assert(sb);

  if(BAD_LEN==(pathlen=path_length(path,0))) {
    warn("%*s...: pathname is longer than allowed\n",path,MAX_PATH_LEN_CHAR-1);
    return 1;
  }

  path_split(path,&dn,&bn);

  if(!dir) {
    if(!(mydir=dir=opendir(dn))) {
      warn("%s: cannot open directory: %s\n",dn,strerror(errno));
      ret=1;
    }
  }

  if(fstatat(dirfd(dir),bn,sb,AT_SYMLINK_NOFOLLOW)) {
    warn("%s: cannot stat: %s\n",path,strerror(errno));
    ret=1;
  }

  if(mydir) closedir(mydir);
  return ret;
}

/* similar_lstat -- uses either fstatfd or lustre_fstatfd to stat a
   file when a DIR* is not available.  You MUST use this function
   instead of stat or lstat otherwise the device and inode numbers
   will be wrong */
int similar_lstat(const char *name,struct stat *statbuf) {
  int statted=0;
  char *bn,*dn;
  DIR *d;
  path_split(name,&dn,&bn);

  if(!(d=opendir(dn))) {
    warn("%s: cannot open directory: %s\n",dn,strerror(errno));
    return 1;
  }

  if(use_lustre_stat) {
    struct dirent *dent;
    while((dent=readdir(d)))
      if(!strcmp(bn,dent->d_name)) {
        if((lustre_lstatfd(d,bn,0,statbuf)))
          warn("%s: cannot stat using lustre stat: %s\n",name,strerror(errno));
        else
          statted=1;
        break;
      }
  } else if((fstatat(dirfd(d),bn,statbuf,AT_SYMLINK_NOFOLLOW)))
      warn("%s: cannot stat: %s\n",name,strerror(errno));
  else
    statted=1;

  closedir(d);
  return !statted;
}

/* rotate_* -- bit rotation functions, equivalent to x<<<y or x>>>y.
   However, C lacks <<< and >>> operators.  These are written in such
   a way that GCC should optimize them to use the bit rotation
   instructions */
static inline uint32_t rotate_left32(uint32_t x,uint32_t y) {
  return (x << y)|(x >> (32-y));
}
static inline uint64_t rotate_left64(uint64_t x,uint64_t y) {
  return (x << y)|(x >> (64-y));
}
static inline uint32_t rotate_right32(uint32_t x,uint32_t y) {
  return (x >> y)|(x << (32-y));
}
static inline uint64_t rotate_right64(uint64_t x,uint64_t y) {
  return (x >> y)|(x << (64-y));
}

/* inthash32/64 -- int hashing functions from:

    http://www.concentric.net/~ttwang/tech/inthash.htm
*/
uint32_t inthash32(uint32_t key) {
  // From http://www.concentric.net/~ttwang/tech/inthash.htm
  // Author Thomas Wang, Jan 1997
  key = ~key + (key << 15); // key = (key << 15) - key - 1;
  key = key ^ rotate_right32(key,12);
  key = key + (key << 2);
  key = key ^ rotate_right32(key,4);
  key = key * 2057; // key = (key + (key << 3)) + (key << 11);
  key = key ^ rotate_right32(key,16);
  return key;
}
uint64_t inthash64(uint64_t key) {
  // From http://www.concentric.net/~ttwang/tech/inthash.htm
  // Author Thomas Wang, Jan 1997
  key = (~key) + (key << 21); // key = (key << 21) - key - 1;
  key = key ^ rotate_right64(key,24);
  key = (key + (key << 3)) + (key << 8); // key * 265
  key = key ^ rotate_right64(key,14);
  key = (key + (key << 2)) + (key << 4); // key * 21
  key = key ^ rotate_right64(key,28);
  key = key + (key << 31);
  return key;
}
