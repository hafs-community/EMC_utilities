#ifndef INC_PARANOIA
#define INC_PARANOIA

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef _ATFILE_SOURCE
#define _ATFILE_SOURCE
#endif

#include <stdlib.h>

/* FAILSAFE MAXIMUMS */
/* The program will refuse to use data that exceeds these maximums.
   This is not strictly necessary, but is simply added as a safeguard
   against buffer overflow attacks. */
#define MAX_GROUP_NAME_LEN 1000   /* maximum length of a group name */
#define MAX_PATH_LEN_CHAR 500000  /* maximum length of a path in characters*/
#define MAX_BASENAME_LEN 20000    /* maximum length of a file basename */
#define MAX_PATH_DEPTH 1000       /* maximum directory recursion depth */
/* NOTE: Strings to store those values must be at least one
   byte larger to hold the terminating null byte. */
/* ANOTHER NOTE: all of these values must be positive and fit into a signed int */

/* Functions to check the three string length assertions above: */
#define group_length(G,should_fail)                                     \
  assert_length(G,1,MAX_GROUP_NAME_LEN,"group name",should_fail) 
#define path_length(P,should_fail)                              \
  assert_length(P,1,MAX_PATH_LEN_CHAR,"path",should_fail)
#define basename_length(B,should_fail)                                  \
  assert_length(B,0,MAX_BASENAME_LEN,"file basename",should_fail)

/* BAD_LEN: returned by the *_length macros in non-failing mode if a
   string length is longer or shorter than allowed */
#define BAD_LEN ((size_t)-1)

/* assert_length: implementation of the *_length macros.  See
   paranoia.c for details */
size_t assert_length(const char *str,size_t min,size_t max,
                     const char *what,int should_fail);

#endif /* INC_PARANOIA */


