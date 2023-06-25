#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif /* _GNU_SOURCE */
#define _ATFILE_SOURCE

#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <pwd.h>
#include <grp.h>
#include <assert.h>
#include <stdio.h>

#include <iomanip>
#include <string>
#include <vector>
#include <sstream>
#include <fstream>
#include <iostream>
#include <ext/hash_set>
#include <ext/hash_map>

#include "basic_utils.h"
#include "disk_usage.h"

using namespace std;
using namespace __gnu_cxx;

/* str_hostname -- returns the hostname as an STL string */
string str_hostname();

/* xmlify/globify -- "cleans" a filename by replacing problematic
   characters (', ", \n, etc.) with safer equivalents */
string xmlify(const string &str);
string globify(const string &str);

/* inthash -- hash function for integers, accepting unsigned 32 or
   64-bit integers */
inline uint32_t inthash(uint32_t u) { return inthash32(u); }
inline uint64_t inthash(uint64_t u) { return inthash64(u); }

/* USAGE_TYPE_* -- used to indicate which of the us_* functions are
   being called */
#define USAGE_TYPE_FSOBJ               1
#define USAGE_TYPE_DIR_UNOPENABLE      2
#define USAGE_TYPE_DIR_TOO_DEEP        3
#define USAGE_TYPE_FILENAME_TOO_LONG   4
#define USAGE_TYPE_PATH_TOO_LONG       5
#define USAGE_TYPE_DUPLICATE_OBJECT    6
#define USAGE_TYPE_DELETED_FSOBJ       7

/* FObjInfo -- a wrapper around a struct stat, which also contains
   additional information that can be calculated from static
   structures.  */
class FObjInfo {
public:
  /* Create a new object from a stat structure */
  FObjInfo(const char *name,const struct stat *s);

  /* Call similar_lstat on a filename to get a stat structure, then
     use it to make a new FObjInfo */
  FObjInfo(const string &name);

  virtual ~FObjInfo();

  /* Comparison operators: is this file the same as another file?
     Uses device and inode numbers. */
  inline bool operator == (const struct stat *s) const {
    return info.st_dev==s->st_dev
      && info.st_ino==s->st_ino;
  }
  inline bool operator == (const FObjInfo &x) const {
    return info.st_dev==x.info.st_dev 
      && info.st_ino==x.info.st_ino;
  }
  inline bool operator !=(const struct stat *s) const { return !(*this==s); }
  inline bool operator !=(const FObjInfo &x) const { return !(*this==x); }

  /* get_path: gets the name of this file */
  inline const string &get_path() const { return dirname; }  

  /* Is this file a directory targeted for usage information? */
  inline bool is_targeted() const {
    if(have_targeted)
      return targeted;
    return targeted=decide_targeted();
  }

  /* return a hash value for this file/dir using its device and inode number */
  inline uint64_t hash() const {
    return inthash(info.st_dev) ^ inthash(info.st_ino);
  }

  /* Return the file/dir's size in bytes according to st_size */
  inline size_t size_bytes() const {
    return info.st_size<0 ? 0 : (size_t)info.st_size;
  }

  /* Special debugging routines */
  inline FObjInfo debug_thing() const {
    return FObjInfo(dirname.c_str(),&info);
  }
  inline void printsomething() const {
    debugn(VERB_DEBUG_HIGH,"%s: dev=%llx ino=%llx targeted=%s\n",
           dirname.c_str(),(unsigned long long)info.st_dev,
           (unsigned long long)info.st_ino,
           is_targeted()?"true":"false");
  }
protected:
  /* decide_targeted: underlying implementation of is_targeted, used
     only if have_targeted is false */
  bool decide_targeted() const;
private:
  string dirname;   // name of this file/dir
  struct stat info; // stat structure for this file/dir

  /* targeted/have_targeted -- targeted is true iff this is a
     directory targeted for usage information.  Have_targeted is true
     if we have determined the value of targeted.  These are mutable
     since they are cached values for a const accessor function */
  mutable bool targeted, have_targeted;
};

/* UserInfo: stores information about a user */
class UserInfo {
public:
  UserInfo(const struct stat *s); // use this stat structure's uid
  UserInfo(uid_t);    // use this specific uid
  virtual ~UserInfo();

  /* return a hash value for this UserInfo: */
  inline uint64_t hash() const { return inthash(uid); }

  /* Accessors: */
  inline uid_t get_uid() const { return uid; }
  inline const string &get_name() const { // underlying implementation in find_name
    if(name.length()==0)
      return find_name();
    else return name;
  }

  /* Comparisons: is this UserInfo referring to the same user as that UserInfo? */
  inline bool operator == (const UserInfo &u) const { return u.uid==uid; }
  inline bool operator != (const UserInfo &u) const { return !((*this)==u); }
protected:
  /* find_name: used to determine the username if it is not yet known */
  const string &find_name() const;
private:
  mutable string name; /* for caching results of find_name */
  uid_t uid;
};

/* GroupInfo: used for storing group IDs.  See UserInfo for
   documentation; the implementation and meanings are exactly the
   same, but with group IDs (gid_t) and group names instead. */
class GroupInfo {
public:
  GroupInfo(const struct stat *s);
  GroupInfo(gid_t);
  virtual ~GroupInfo();

  inline uint64_t hash() const { return inthash(gid); }
  inline gid_t get_gid() const { return gid; }
  inline const string &get_name() const {
    if(name.length()==0)
      return find_name();
    else return name;
  }

  inline bool operator == (const GroupInfo &u) const { return u.gid==gid; }
  inline bool operator != (const GroupInfo &u) const { return !((*this)==u); }
protected:
  const string &find_name() const;
private:
  mutable string name; /* for caching results of getgrgid */
  gid_t gid;
};

/* UsageInfo: records disk usage information for a set of files.  This
   object has no idea what the set of files may be.  All it knows is
   the statistics it has collected. */
class UsageInfo {
public:
  UsageInfo();
  virtual ~UsageInfo();

  /* add -- add this file's statistics to the usage information collected. 
       s -- the stat structure for the file
       type -- which type of statistics are being requested?
         Must be one of USAGE_TYPE_* near the top of this file.
  */
  void add(const struct stat *s,int type);

  /* clear -- clear all usage statistics */
  void clear();

  /* xml_report -- generate an XML report on the usage, and send it to
     ostream &o.  The indent is prepended to each line */
  void xml_report(ostream &o,const string &indent="") const;
private:
  /* MEMBER VARIABLES

     regulars/dirs/links/others -- how many files of this type were seen?
     bytes -- how many bytes of files were seen (from st_size)?
     latest_a/m/c -- latest atime, mtime and ctime seen in a regular file

     world_writable -- number of world-writable non-symlinks seen
     setuid_file, set_gid file -- number of regular files with setuid/setgid

     big_files -- number of "big" files as determined by big_file_size

     based on the type parameter to add(s,type), these are recorded:

     dir_unopenable -- number of directories that could not be opendirred
     dir_too_deep -- number of directories past max recursion depth
     filename_too_long,path_too_long -- number of strings that were too long

     duplicate_objects -- number of duplicate device/inode pairs
     deleted_fsobj -- number of filesystem objects deleted
  */
  size_t regulars,dirs,links,others,bytes;
  time_t latest_a,latest_m,latest_c;
  size_t world_writable,setuid_file,setgid_file;
  size_t big_files;
  size_t dir_unopenable,dir_too_deep,filename_too_long,path_too_long;
  size_t duplicate_objects,deleted_fsobj;
};

// hash function wrappers for __gnu_cxx::hash_set and hash_map:
namespace __gnu_cxx {
template<> struct hash<GroupInfo> {
  size_t operator() (const GroupInfo &value) const {
    return value.hash();
  };
};
template<> struct hash<UserInfo> {
  size_t operator() (const UserInfo &value) const {
    return value.hash();
  };
};
template<> struct hash<FObjInfo> {
  size_t operator() (const FObjInfo &value) const {
    return value.hash();
  };
};
}

/* typedefs needed for static members: */
typedef vector<FObjInfo> FObjList;
typedef hash_set<FObjInfo> FObjSet;
typedef vector<FObjInfo>::iterator dir_iterator;

typedef hash_map<UserInfo,UsageInfo> UserUsage;
typedef hash_map<FObjInfo,UsageInfo> DirUsage;
typedef hash_map<FObjInfo,UserUsage> DirUserUsage;
typedef hash_map<UserInfo,DirUsage> UserDirUsage;

typedef hash_map<GroupInfo,UsageInfo> GroupUsage;
typedef hash_map<FObjInfo,GroupUsage> DirGroupUsage;
typedef hash_map<UserInfo,GroupUsage> UserGroupUsage;
typedef hash_map<GroupInfo,UserUsage> GroupUserUsage;
typedef hash_map<FObjInfo,GroupUserUsage> DirGroupUserUsage;

/**********************************************************************/
/**********************************************************************/

/* IMPLEMENTATION */

/**********************************************************************/
/**********************************************************************/

/* GLOBALS */

FILE *file_lister=NULL;
int list_all_files=0;

static hash_set<FObjInfo> target_dirs;
static FObjList dir_stack;

static DirUserUsage dir_user_usage;
static UserDirUsage user_dir_usage;
static DirUsage dir_usage;
static UserUsage user_usage;
static UsageInfo all_usage;

static GroupUsage group_usage;
static DirGroupUsage dir_group_usage;
static UserGroupUsage user_group_usage;
static GroupUserUsage group_user_usage;
static DirGroupUserUsage dir_group_user_usage;

/* Parameters settable by us_* routines */
static size_t big_file_size=104857600;
static FObjSet big_files;

/* Output streams for "big file" listings */
static ofstream big_glob_report, big_print0_report, big_text_report, big_xml_report;

/* How many "big file" FObjInfo objects can we cache before writing
   them out to the "big file" listing files: */
static size_t max_big_files_in_mem=3000;

/**********************************************************************/

/* Set or get the big_file_size and list_all_files flag */

void us_set_big_file_size(size_t size) {
  big_file_size=size;
}
size_t us_get_big_file_size() {
  return big_file_size;
}

void us_list_all_files(int shouldi) {
  list_all_files=shouldi;
}
int us_get_list_all_files() {
  return list_all_files;
}

/**********************************************************************/

/* xml_report: numerous implementations of this for various types.  
   These routines write an object to an XML file.
     o -- the XML file output stream
     second parameter -- the object to write
     indent -- string to prepend to each line
*/

void xml_report(ostream &o,const UsageInfo &u,const string &indent="") {
  u.xml_report(o,indent);
}

template<class T>
void xml_report(ostream &o,const hash_map<GroupInfo,T> &u,const string &indent="") {
  string more_indent=indent+"  ";
  typename hash_map<GroupInfo,T>::const_iterator i,e;
  i=u.begin();
  e=u.end();
  for(;i!=e;i++) {
    o<<indent<<"<group_usage id=\""<<i->first.get_gid()
     <<"\" name=\""<<xmlify(i->first.get_name())<<"\">"<<endl;
    xml_report(o,i->second,more_indent);
    o<<indent<<"</group_usage>"<<endl;
  }
}

template<class T>
void xml_report(ostream &o,const hash_map<UserInfo,T> &u,const string &indent="") {
  string more_indent=indent+"  ";
  typename hash_map<UserInfo,T>::const_iterator i,e;
  i=u.begin();
  e=u.end();
  for(;i!=e;i++) {
    o<<indent<<"<user_usage id=\""<<i->first.get_uid()
     <<"\" name=\""<<xmlify(i->first.get_name())<<"\">"<<endl;
    xml_report(o,i->second,more_indent);
    o<<indent<<"</user_usage>"<<endl;
  }
}

template<class T>
void xml_report(ostream &o,const hash_map<FObjInfo,T> &d,const string &indent="") {
  string more_indent=indent+"  ";
  typename hash_map<FObjInfo,T>::const_iterator i,e;
  i=d.begin();
  e=d.end();
  for(;i!=e;i++) {
    o<<indent<<"<dir_usage path=\""<<xmlify(i->first.get_path())<<"\">"<<endl;
    xml_report(o,i->second,more_indent);
    o<<indent<<"</dir_usage>"<<endl;
  }
}

/* update_bigfile_reports -- updates the list of "big" files.  Once
   the number of such big files listed in the in-memory cahce exceeds
   "max," the data is written out, and the cache is cleared */
void update_bigfile_reports(FObjSet &b,size_t max=max_big_files_in_mem) {
  if(b.size()>max) {
    FObjSet::const_iterator i,e;
    i=b.begin();
    e=b.end();
    for(;i!=e;i++) {
      big_glob_report<<globify(i->get_path())<<endl;
      big_print0_report<<i->get_path()<<'\0';
      big_text_report<<i->get_path()<<endl;
      big_xml_report<<"  <bigfile size=\""<<i->size_bytes()<<"\">"
       <<xmlify(i->get_path())<<"</bigfile>"<<endl;
    }
    b.clear();
  }
}

/* start_(whatever)_report -- opens the output stream for the big file
   report in the listed file format.  All of these must be called
   before update_bigfile_report

   pre -- prefix to prepend to filenames
   type -- string appended to pre, followed by an extension (.txt or whatever)

*/
void start_glob_report(const string &pre,const string &type) {
  string where(pre+type+".glob");
  try {
    big_glob_report.open(where.c_str());
  } catch(const exception &e) {
    cerr<<where<<": cannot start reporting: "<<e.what()<<endl;
  } catch(...) {
    cerr<<where<<": cannot start reporting (reason unknown)"<<endl;
  }
}

void start_text_report(const string &pre,const string &type) {
  string where(pre+type+".txt");
  try {
    big_text_report.open(where.c_str());
  } catch(const exception &e) {
    cerr<<where<<": cannot start reporting: "<<e.what()<<endl;
  } catch(...) {
    cerr<<where<<": cannot start reporting (reason unknown)"<<endl;
  }
}

void start_print0_report(const string &pre,const string &type) {
  string where(pre+type+".print0");
  try {
    big_print0_report.open(where.c_str());
  } catch(const exception &e) {
    cerr<<where<<": cannot start reporting: "<<e.what()<<endl;
  } catch(...) {
    cerr<<where<<": cannot start reporting (reason unknown)"<<endl;
  }
}

void start_xml_report(const string &pre,const string &type,
                      double start_time,size_t max_depth) {
  uid_t uid=getuid(),euid=geteuid();
  UserInfo user(uid),euser(euid);
  string where(pre+type+".xml");
  string hostname=str_hostname();
  try {
    big_xml_report.open(where.c_str());
    big_xml_report<<"<?xml version=\"1.0\"?>"<<endl
                  <<endl
                  <<"<big_file_list big_file_size=\""<<big_file_size<<"\""
                  <<" start=\""<<setprecision(16)<<start_time<<"\""
                  <<" uid=\""<<uid<<"\" user=\""<<xmlify(user.get_name())<<"\""
                  <<" euid=\""<<euid<<"\" euser=\""<<xmlify(euser.get_name())<<"\""
                  <<" host=\""<<xmlify(hostname)<<"\""
                  <<" max_depth=\""<<max_depth<<"\""
                  <<">"<<endl;
  } catch(const exception &e) {
    cerr<<where<<": cannot start reporting: "<<e.what()<<endl;
  } catch(...) {
    cerr<<where<<": cannot start reporting (reason unknown)"<<endl;
  }
}

/* gen_xml_report -- generate one of the final XML reports

   pre -- prefix to prepend to filenames
   type -- which type of report is this? (by-user-usage, whatever)
           This is appended to the "pre" string
   t -- the object to XMLify using xml_report functions
   start_time/end_time -- start and end times of the filesystem
        walker
   maxdepth -- maximum directory recursion depth
*/
template<class T>
void gen_xml_report(const string &pre,const string &type,const T &t,
                    double start_time,double end_time,size_t maxdepth) {
  uid_t uid=getuid(),euid=geteuid();
  UserInfo user(uid),euser(euid);
  string where=pre+type+".xml",indent("  ");
  string xmltype=type;
  string hostname=str_hostname();

  for(string::iterator i=xmltype.begin(),e=xmltype.end();i!=e;i++)
    if(!isalnum(*i) && *i!='_')
      *i='_';

  string element_name=xmltype;


  debug("%s: generate XML report of type %s...\n",where.c_str(),type.c_str());

  std::ofstream o(where.c_str());
  o<<"<?xml version=\"1.0\"?>"<<endl<<endl;
  o<<"<"<<element_name
   <<" start=\""<<setprecision(16)<<start_time<<"\""
   <<" end=\""<<setprecision(16)<<end_time<<"\""
   <<" uid=\""<<uid<<"\" user=\""<<xmlify(user.get_name())<<"\""
   <<" euid=\""<<euid<<"\" euser=\""<<xmlify(euser.get_name())<<"\""
   <<" host=\""<<xmlify(hostname)<<"\""
   <<" max_depth=\""<<maxdepth<<"\""
   <<">"<<endl;
  xml_report(o,t,indent);
  o<<"</"<<element_name<<">"<<endl;

  debug("%s: done generating %s XML report.\n",where.c_str(),type.c_str());
}

/**********************************************************************/

/* add_usage -- add this file to the usage statistics, using a specific mode.
   path -- path to the file
   s -- stat structure for the file
   type -- one of the USAGE_TYPE_* which indicate which us_* function was called.
*/
void add_usage(const char *path,const struct stat *s,int type) {
  typedef unsigned long long ull;
  UserInfo u(s);
  GroupInfo g(s);
  all_usage.add(s,type);
  user_usage[u].add(s,type);
  group_usage[g].add(s,type);
  user_group_usage[u][g].add(s,type);
  group_user_usage[g][u].add(s,type);

  for(dir_iterator i=dir_stack.begin(),e=dir_stack.end();i!=e;i++) {
    dir_user_usage[*i][u].add(s,type);
    user_dir_usage[u][*i].add(s,type);
    dir_group_usage[*i][g].add(s,type);
    dir_group_user_usage[*i][g][u].add(s,type);
    dir_usage[*i].add(s,type);
  }

  if(type==USAGE_TYPE_FSOBJ) {
    if((int64_t)s->st_size>(int64_t)big_file_size)
      big_files.insert(FObjInfo(path,s));
    if(list_all_files && file_lister) {
      char type='?';
      if(S_ISDIR(s->st_mode))
        type='d';
      else if(S_ISREG(s->st_mode))
        type='-';
      else if(S_ISLNK(s->st_mode))
        type='l';
      fprintf(file_lister,"%c %04o %llu %llu %llu %llu %s %s %s\n",
              type,(int)(s->st_mode & 07777),
              (ull)s->st_ctime, (ull)s->st_mtime, (ull)s->st_atime,
              (ull)s->st_size,
              u.get_name().c_str(),g.get_name().c_str(),
              path);
    }
  }
}

/* us_dir_enter -- see disk_usage.h. */
void us_dir_enter(const char *dirname,const struct stat *s) {
  try {
    FObjInfo di(dirname,s);
    //  di.printsomething();
    if(di.is_targeted()) {
      debug("%s: is targeted for disk usage\n",dirname);
      di.printsomething();
      dir_stack.push_back(FObjInfo(dirname,s));
    } else {
      di.printsomething();
      debugn(VERB_DEBUG_HIGH,"%s: not targeted for disk usage\n",dirname);
    }
  } catch(const exception &e) {
    cerr<<dirname<<": error updating usage when entering directory: "<<e.what()<<endl;
  } catch(...) {
    cerr<<dirname<<": unknown error updating usage when entering directory"<<endl;
  }
}

/* us_dir_leave -- see disk_usage.h. */
void us_dir_leave(const char *dirname,const struct stat *s) {
  try {
    if(!dir_stack.empty() && FObjInfo(dirname,s)==dir_stack.back()) {
      debug("%s: leaving this directory\n",dirname);
      dir_stack.pop_back();
    }
  } catch(const exception &e) {
    cerr<<dirname<<": error updating usage while leaving directory: "<<e.what()<<endl;
  } catch(...) {
    cerr<<dirname<<": unknown error updating usage while leaving directory"<<endl;
  }
}

/* us_file_found -- see disk_usage.h */
void us_file_found(const char *filename,const struct stat *s) {
  try {
    add_usage(filename,s,USAGE_TYPE_FSOBJ);
    update_bigfile_reports(big_files);
  } catch(const exception &e) {
    cerr<<filename<<": error updating usage stats: "<<e.what()<<endl;
  } catch(...) {
    cerr<<filename<<": unknown error updating usage stats"<<endl;
  }
}

/* us_file_deleted -- see disk_usage.h */
void us_file_deleted(const char *filename,const struct stat *s) {
  try {
    add_usage(filename,s,USAGE_TYPE_DELETED_FSOBJ);
  } catch(const exception &e) {
    cerr<<filename<<": error updating usage stats: "<<e.what()<<endl;
  } catch(...) {
    cerr<<filename<<": unknown error updating usage stats"<<endl;
  }
}

/* us_dir_unopenable -- see disk_usage.h */
void us_dir_unopenable(const char *filename,const struct stat *s) {
  try {
    add_usage(filename,s,USAGE_TYPE_DIR_UNOPENABLE);
  } catch(const exception &e) {
    cerr<<filename<<": error updating usage stats for unopenable directory: "<<e.what()<<endl;
  } catch(...) {
    cerr<<filename<<": unknown error updating usage stats for unopenable directory"<<endl;
  }

}

/* us_dir_too_deep -- see disk_usage.h */
void us_dir_too_deep(const char *filename,const struct stat *s) {
  try {
    add_usage(filename,s,USAGE_TYPE_DIR_TOO_DEEP);
  } catch(const exception &e) {
    cerr<<filename<<": error updating usage stats for overly deep directory: "<<e.what()<<endl;
  } catch(...) {
    cerr<<filename<<": unknown error updating usage stats for overly deep directory"<<endl;
  }
}

/* us_duplicate_object -- see disk_usage.h */
void us_duplicate_object(const char *filename,const struct stat *s) {
  try {
    add_usage(filename,s,USAGE_TYPE_DUPLICATE_OBJECT);
  } catch(const exception &e) {
    cerr<<filename<<": error updating usage stats for overly deep directory: "<<e.what()<<endl;
  } catch(...) {
    cerr<<filename<<": unknown error updating usage stats for overly deep directory"<<endl;
  }
}

/* us_filename_too_long -- see disk_usage.h */
void us_filename_too_long(const char *dirname,const char *filepart,
                          const struct stat *s) {
  try {
    add_usage("**unspecified**",s,USAGE_TYPE_FILENAME_TOO_LONG);
  } catch(const exception &e) {
    cerr<<dirname<<"/(long filename): error updating usage stats for overly-long filename: "<<e.what()<<endl;
  } catch(...) {
    cerr<<dirname<<"/(long filename): unknown error updating usage stats for overly-long filename"<<endl;
  }

}

/* us_path_too_long -- see disk_usage.h */
void us_path_too_long(const char *dirname,const char *filepart,const struct stat *s) {
  try {
    add_usage("**unspecified**",s,USAGE_TYPE_PATH_TOO_LONG);
  } catch(const exception &e) {
    cerr<<dirname<<"/(long filename): error updating usage stats for overly-long path: "<<e.what()<<endl;
  } catch(...) {
    cerr<<dirname<<"/(long filename): unknown error updating usage stats for overly-long path"<<endl;
  }
}

/* us_add_dir -- see disk_usage.h */
void us_add_dir(const char *dirname) {
  try {
    FObjInfo di(dirname);
    target_dirs.insert(di);
    assert(di.is_targeted());
    FObjInfo di2=di.debug_thing();
    assert(di2.is_targeted());
    debug("%s: now targeted for disk usage\n",dirname);
    di.printsomething();
    di2.printsomething();
  } catch(const exception &e) {
    cerr<<dirname<<": cannot add directory to usage dir list: "<<e.what()<<endl;
  } catch(...) {
    cerr<<dirname<<": unknown error adding directory to usage dir list"<<endl;
  }
}

/* us_start_reports -- see disk_usage.h */
void us_start_reports(const char *prefix,double start_time,size_t max_depth) {
  try {
    string pre=prefix;
    start_glob_report(pre,"big-files");
    start_print0_report(pre,"big-files");
    start_xml_report(pre,"big-files",start_time,max_depth);
    start_text_report(pre,"big-files");
    if(list_all_files) {
      string where=pre+"all-files.lst";
      if(!(file_lister=fopen(where.c_str(),"wt"))) {
        warn("%s: cannot open for text writing: %s\n",
             where.c_str(),strerror(errno));
      } else
        fprintf(file_lister,"type mode ctime mtime atime size user group path\n");
    }
  } catch(const exception &e) {
    cerr<<prefix<<": cannot start reporting (2): "<<e.what()<<endl;
  } catch(...) {
    cerr<<prefix<<": cannot start reporting (2, reason unknown)"<<endl;
  }
}

/* us_generate_reports -- see disk_usage.h */
void us_generate_reports(const char *prefix,double start_time,double end_time,size_t max_depth) {
  try {
    string pre(prefix);
    gen_xml_report(pre,"all-usage",all_usage,start_time,end_time,max_depth);

    gen_xml_report(pre,"per-dir-usage",dir_usage,start_time,end_time,max_depth);
    gen_xml_report(pre,"per-user-usage",user_usage,start_time,end_time,max_depth);
    gen_xml_report(pre,"by-dir-user-usage",dir_user_usage,start_time,end_time,max_depth);
    gen_xml_report(pre,"by-user-dir-usage",user_dir_usage,start_time,end_time,max_depth);

    gen_xml_report(pre,"per-group-usage",group_usage,start_time,end_time,max_depth);
    gen_xml_report(pre,"by-dir-group-usage",dir_group_usage,start_time,end_time,max_depth);
    gen_xml_report(pre,"by-user-group-usage",user_group_usage,start_time,end_time,max_depth);
    gen_xml_report(pre,"by-group-user-usage",group_user_usage,start_time,end_time,max_depth);
    gen_xml_report(pre,"by-dir-group-user-usage",dir_group_user_usage,start_time,end_time,max_depth);

    update_bigfile_reports(big_files,0);

    big_glob_report.close();
    big_print0_report.close();
    big_text_report.close();
    big_xml_report<<"</big_file_list>"<<endl;
    big_xml_report.close();
    if(list_all_files && file_lister)
      if(fclose(file_lister))
        warn("%sall-files.lst: error closing; file may be incomplete: %s\n",prefix,strerror(errno));
  } catch(const exception &e) {
    cerr<<prefix<<": cannot generate reports: "<<e.what()<<endl;
  } catch(...) {
    cerr<<prefix<<": cannot generate reports (reason unknown)"<<endl;
  }
}

/* us_reset -- see disk_usage.h */
void us_reset() {
  try {
    dir_user_usage.clear();
    user_dir_usage.clear();
    dir_usage.clear();
    user_usage.clear();
    all_usage.clear();

    group_usage.clear();
    dir_group_usage.clear();
    user_group_usage.clear();
    group_user_usage.clear();
    dir_group_user_usage.clear();
  } catch(const exception &e) {
    cerr<<"Cannot reset usage stats: "<<e.what()<<endl;
  } catch(...) {
    cerr<<"Unknown exception when resetting usage stats."<<endl;
  }
}

/**********************************************************************/

/* Implementation of FObjInfo -- see the class definition for documentation */

FObjInfo::FObjInfo(const char *name,const struct stat *s):
  dirname(name),info(),targeted(false),have_targeted(false)
{
  memcpy(&info,s,sizeof(struct stat));
  printsomething();
}
FObjInfo::FObjInfo(const string &name):
  dirname(name),info(),targeted(false),have_targeted(false)
{
  if(similar_lstat(name.c_str(),&info))
    fail("%s: cannot stat: %s\n",name.c_str(),strerror(errno));
  printsomething();
}
FObjInfo::~FObjInfo() {}
bool FObjInfo::decide_targeted() const {
  return target_dirs.find(*this)!=target_dirs.end();
}

/**********************************************************************/

/* Implementation of UserInfo; see the class def. for documentation */

UserInfo::UserInfo(const struct stat *s): uid(s->st_uid) {}
UserInfo::UserInfo(uid_t u): uid(u) {}
UserInfo::~UserInfo() {}
const string &UserInfo::find_name() const {
  struct passwd *pwd=getpwuid(uid);
  if(pwd && pwd->pw_name)
    return name=pwd->pw_name;
  ostringstream oss;
  oss<<uid;
  return name=oss.str();
}

/**********************************************************************/

/* Implementation of GroupInfo; see the class def. for documentation */

GroupInfo::GroupInfo(const struct stat *s): gid(s->st_gid) {}
GroupInfo::GroupInfo(uid_t g): gid(g) {}
GroupInfo::~GroupInfo() {}
const string &GroupInfo::find_name() const {
  struct group *g=getgrgid(gid);
  if(g && g->gr_name)
    return name=g->gr_name;
  ostringstream oss;
  oss<<gid;
  return name=oss.str();
}

/**********************************************************************/

/* Implementation of UsageInfo; see the class def. for documentation */
UsageInfo::UsageInfo():
  // Initialize all counters to 0
  // NOTE: MAKE SURE THESE MATCH clear()
  regulars(0),dirs(0),links(0),others(0),bytes(0),
  latest_a(0),latest_m(0),latest_c(0),
  world_writable(0),setuid_file(0),setgid_file(0),
  big_files(0),
  dir_unopenable(0),dir_too_deep(0),filename_too_long(0),
  path_too_long(0),duplicate_objects(0), deleted_fsobj(0)
{}

UsageInfo::~UsageInfo() {}
void UsageInfo::clear() {
  // Clear: initialize all variables to the original values from UsageInfo()
  regulars=0; dirs=0; links=0; others=0; bytes=0;
  latest_a=0; latest_m=0; latest_c=0;
  world_writable=0; setuid_file=0; setgid_file=0;
  big_files=0;
  dir_unopenable=0; dir_too_deep=0; filename_too_long=0;
  path_too_long=0; duplicate_objects=0; deleted_fsobj=0;
}
void UsageInfo::add(const struct stat *s,int type) {
  // First, handle the various weird USAGE_TYPEs:
  switch(type) {
  case USAGE_TYPE_DIR_UNOPENABLE:    dir_unopenable++;    return;
  case USAGE_TYPE_DIR_TOO_DEEP:      dir_too_deep++;      return;
  case USAGE_TYPE_FILENAME_TOO_LONG: filename_too_long++; return;
  case USAGE_TYPE_PATH_TOO_LONG:     path_too_long++;     return;
  case USAGE_TYPE_DUPLICATE_OBJECT:  duplicate_objects++; return;
  case USAGE_TYPE_DELETED_FSOBJ:     deleted_fsobj++;     return;
  default: break;
  }

  // High_bit_check: set to true if we need to check for setgid/setuid:
  bool high_bit_check=false;

  if(S_ISREG(s->st_mode)) {
    regulars++;
    high_bit_check=true;
  } else if(S_ISDIR(s->st_mode))
    dirs++;
  else if(S_ISLNK(s->st_mode))
    links++;
  else {
    // Not a symlink, directory or regular file.  We will still check
    // for setgid/setuid bits, just in case someone goes crazy and
    // sets setgid and world execute on a socket, or some such
    // craziness.
    high_bit_check=true;
    others++;
  }

  // Check for "big" files:
  if(S_ISREG(s->st_mode) && big_file_size>0 &&
     (int64_t)s->st_size>(int64_t)big_file_size)
    big_files++;

  // Record the size of anything based on its lstat st_size:
  if(s->st_size>0)
    bytes+=s->st_size;

  // Only check world-writable files if they are NOT symlinks:
  if(0!=(s->st_mode&0002) && !S_ISLNK(s->st_mode))
    world_writable++;

  // Check for setuid or setgid for files that have high_bit_check set:
  if(high_bit_check) {
    if((s->st_mode & S_ISUID))
      setuid_file++;
    if((s->st_mode & S_ISGID))
      setgid_file++;
  }

  // For regular files, update the latest mtime, atime and ctime seen:
  if(S_ISREG(s->st_mode)) {
    if(s->st_mtime>latest_m)
      latest_m=s->st_mtime;
    if(s->st_atime>latest_a)
      latest_a=s->st_atime;
    if(s->st_ctime>latest_c)
      latest_c=s->st_ctime;
  }
}

void UsageInfo::xml_report(ostream &o,const string &indent) const {
  /* Generate an XML usage report inside a <usage> element */
  o<<indent<<"<usage>"<<endl;

  /* Filesystem object type counts */
  o<<indent<<"  <fsobject total=\""<<(regulars+dirs+links+others)
   <<"\" regulars=\""<<regulars<<"\" dirs=\""<<dirs<<"\" links=\""<<links
   <<"\" others=\""<<others<<"\"/>"<<endl;

  /* Total size in bytes */
  o<<indent<<"  <space bytes=\""<<bytes<<"\"/>"<<endl;

  /* Most recent m/c/a times */
  o<<indent<<"  <latest mtime=\""<<latest_m<<"\" ctime=\""<<latest_c
   <<"\" atime=\""<<latest_a<<"\"/>"<<endl;

  /* Naughty file stats */
  if(world_writable||setuid_file||setgid_file) {
    o<<indent<<"  <badperm";
    if(world_writable) o<<" world_writable=\""<<world_writable<<"\"";
    if(setuid_file) o<<" setuid_file=\""<<setuid_file<<"\"";
    if(setgid_file) o<<" setgid_file=\""<<setgid_file<<"\"";
    o<<"/>"<<endl;
  }

  /* Big file stats */
  if(big_files)
    o<<indent<<"  <bigfile threshold=\""<<big_file_size<<"\" count=\""<<big_files<<"\"/>"<<endl;

  /* deleted files */
  if(deleted_fsobj)
    o<<indent<<"  <deletions count=\""<<deleted_fsobj<<"\"/>"<<endl;

  /* Access restriction stats */
  if(dir_unopenable||dir_too_deep||filename_too_long||path_too_long||duplicate_objects) {
    o<<indent<<"  <no_access";
    if(dir_unopenable) o<<" dir_unopenable=\""<<dir_unopenable<<"\"";
    if(dir_too_deep) o<<" dir_too_deep=\""<<dir_too_deep<<"\"";
    if(filename_too_long) o<<" filename_too_long=\""<<filename_too_long<<"\"";
    if(path_too_long) o<<" path_too_long=\""<<path_too_long<<"\"";
    if(duplicate_objects) o<<" duplicate_objects=\""<<duplicate_objects<<"\"";
    o<<"/>"<<endl;
  }

  /* End the element */
  o<<indent<<"</usage>"<<endl;
}

/**********************************************************************/

// Stream manipulator that resets all ios_base flags:
ostream &resetall(ostream &o) {
  o<<resetiosflags(ios_base::boolalpha|ios_base::dec|ios_base::fixed|ios_base::hex|ios_base::internal|ios_base::left|ios_base::oct|ios_base::right|ios_base::scientific|ios_base::showbase|ios_base::showpoint|ios_base::showpos|ios_base::skipws|ios_base::unitbuf|ios_base::uppercase|ios_base::adjustfield|ios_base::basefield|ios_base::floatfield)<<setw(0);
  return o;
}

// Stream manipulator that sets all exception flags except goodbit (success):
ostream &exceptall(ostream &o) {
  o.exceptions(ofstream::eofbit|ofstream::failbit|ofstream::badbit);
  return o;
}

string xmlify(const string &str) { // turn string into an XML-okay version
  string::const_iterator from=str.begin(),end=str.end();
  ostringstream out;
  for(;from!=end;from++) {
    if(*from<33 || *from>126) {
      out<<"&#"<<setw(4)<<setfill('0')<<right<<hex<<int(*from)<<resetall<<';';
    } else {
      switch(*from) {
      case '"':  out<<"&quot;"; break;
      case '\'': out<<"&apos;"; break;
      case '&':  out<<"&amp;";  break;
      case '<':  out<<"&lt;" ;  break;
      case '>':  out<<"&gt;" ;  break;
      default:
        out<<*from;
        break;
      }
    }
  }
  return out.str();
}

string globify(const string &str) { // turn string into a glob-okay version
  string::const_iterator from=str.begin(),end=str.end();
  ostringstream out;
  for(;from!=end;from++) {
    if(*from<33 || *from>126) {
      out<<"[\\"<<setw(3)<<setfill('0')<<right<<oct<<int(*from)<<resetall<<']';
    } else {
      switch(*from) {
      case '[':  out<<"[\\[]"; break;
      case '{':  out<<"[\\{]"; break;
      case ']':  out<<"[\\]]"; break;
      case '}':  out<<"[\\}]"; break;
      case '*':  out<<"[\\*]"; break;
      case '+':  out<<"[\\+]"; break;
      case '?':  out<<"[\\?]"; break;
      case '\\': out<<"[\\\\]";  break;
      default:
        out<<*from;
        break;
      }
    }
  }
  return out.str();
}

string str_hostname() { // get hostname as an std::string
  size_t hostnamelen=300;
  char hostname[hostnamelen];
  if(gethostname(hostname,hostnamelen))
    strcpy(hostname,"unknown");
  else
    hostname[hostnamelen-1]='\0';
  return string(hostname);
}

