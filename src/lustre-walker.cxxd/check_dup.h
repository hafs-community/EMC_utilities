#ifndef INC_CHECK_DUP
#define INC_CHECK_DUP

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef _ATFILE_SOURCE
#define _ATFILE_SOURCE
#endif

#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
  
  /* hit_file: returns 1 if hit_file has been called on this file
     before, 0 otherwise. */
  int hit_file(dev_t device,ino_t inode);

#ifdef __cplusplus
}
#endif

#endif /* INC_CHECK_DUP */
