#ifndef INC_BASIC_UTILS
#define INC_BASIC_UTILS

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef _ATFILE_SOURCE
#define _ATFILE_SOURCE
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <stdarg.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

  /* verbosity and logging routines: used to write out debug or
     warning messages, and allow the user to control which ones are
     written.

       set_verbosity -- sets the verbosity level to a specific amount
       increment_verbosity -- become more verbose

       fail -- abort the program with a message sent to stderr

       warn/debug/debugn -- print a message at a specific verbosity level */
  void set_verbosity(int level);
  void increment_verbosity();
  void fail(const char *format,...);
  void warn(const char *format,...);
  void debug(const char *format,...);
  void debugn(int level,const char *format,...);

  /* Verbosity levels: */

#define VERB_DEBUG_HIGH 3
#define VERB_DEBUG 2
#define VERB_WARN 1
#define VERB_FATAL 0

  /* fulltime -- return the unix epoch time including microseconds */
  double fulltime();

  /* set/get use_lustre_stat: should we use the Lusre stat implementation? */
  void set_use_lustre_stat(int yesno);
  int get_use_lustre_stat(void);

  /* similar_lstat: an lstat implementation similar to the one used by
     the filesystem walker implementation.  This is needed as a
     workaround for a bug in the Lustre filesystem: the inode and
     device numbers for a file depend on how you stat the file. */
  int similar_lstat(const char *name,struct stat *sb);

  /* Implementation of similar_lstat; don't call these two directly
     unless you know what you're doing.  See basic_utils.c for details. */
  int lustre_lstatfd(DIR *dir,const char *name,size_t pathlen,struct stat *sb);
  int parent_fstatfd(DIR *dir,const char *name,size_t pathlen,struct stat *sb);

  /* inthash32/64: integer hash functions */
  uint32_t inthash32(uint32_t key);
  uint64_t inthash64(uint64_t key);

#ifdef __cplusplus
}
#endif

#endif /* INC_BASIC_UTILS */

