#define _GNU_SOURCE
#define _ATFILE_SOURCE

#include <assert.h>
#include <string.h>
#include <stdlib.h>

#include "basic_utils.h"
#include "paranoia.h"

/* assert_length: get the length of string str, subject to certain
   limitations.  This routine is never called directly, and instead is
   called through C macros

      min/max -- abort via fail() or return BAD_LEN if the string is
        not within this range of lengths.  The strnlen function is
        used instead of strlen, with a maximum search length of max.

      what -- what type of string is this?  Used to make failure messages 
        more meaningful.

      should_fail -- should we abort on error?  non-zero=yes, zero=return
        BAD_LEN instead.
*/
size_t assert_length(const char *str,size_t min,size_t max,const char *what,int should_fail) {
  size_t len;
  assert(what);
  if(!str)
    fail("Null %s\n",what);
  len=strnlen(str,max+1);
  if(len<min) {
    if(should_fail)
      fail("%*s: %s invalid: has less than %llu character(s)",str,(int)len,what,(unsigned long long)min);
    else
      return BAD_LEN;
  }
  if(len>max) {
    if(should_fail)
      fail("%*s...: %s invalid: has more than %llu characters",str,(int)max,what,(unsigned long long)max);
    else
      return BAD_LEN;
  }
  return len;
}

