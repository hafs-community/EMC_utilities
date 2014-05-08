#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef _ATFILE_SOURCE
#define _ATFILE_SOURCE
#endif

#include <utility>
#include <ext/hash_set>

#include "check_dup.h"
#include "basic_utils.h"

using namespace std;
using namespace __gnu_cxx;

typedef std::pair<dev_t,ino_t> devino;

namespace __gnu_cxx {
  /* hash<devino>: implementation of the hash function for
     device/inode number pairs.  This is needed for the "which files
     have I seen" hashtable. */
template<> struct hash<devino> {
  size_t operator() (const devino &value) const {
    return inthash64(value.first) ^ inthash64(value.second);
  };
};
}

/* hits -- a set of files seen so far, identified only by the
   device/inode number pair.  This is implemented by a GNU C++
   hash_set, which is a hashtable implementation of a set. */
static hash_set<devino> hits;

/* hit_file: called to indicate that a specific file has been seen.
   Returns 1 if we already saw the file before now, or 0 if we
   didn't. */
int hit_file(dev_t device,ino_t inode) {
  try {
    devino di(device,inode);
    if(hits.find(di)==hits.end()) {
      hits.insert(di);
      return 0;
    } else
      return 1;
  } catch(...) {
    return 0;
  }
}
