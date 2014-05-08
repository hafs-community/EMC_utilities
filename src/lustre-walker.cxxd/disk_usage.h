#ifndef INC_DISK_USAGE
#define INC_DISK_USAGE

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef _ATFILE_SOURCE
#define _ATFILE_SOURCE
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

  /* us_list_files: list all files, plus size, mtime, etc. */
  void us_list_all_files(int shouldi);
  int us_get_list_all_files(); /* accessor */

  /* us_add_dir: call this to add a directory to the list of those for
     which you want usage statistics */
  void us_add_dir(const char *dirname);

  /* us_start_reports: use this to prepare to generate usage
     statistics reports.  You must call this after all us_add_dir
     calls, but before calling any other functions. */
  void us_start_reports(const char *prefix,double start_time,size_t max_depth);

  /* us_generate_reports: call this to generate usage statistics
     reports.  The report filenames will have the specified prefix
     prepended to them with no interveneing "/".  */
  void us_generate_reports(const char *prefix,
                           double start_time,
                           double end_time,
                           size_t max_depth);

  /* Set or query the size of a file that considered "too large" */
  void us_set_big_file_size(size_t size);
  size_t us_get_big_file_size();

  /* us_reset: reset all usage statistics.  Use this to generate
     statistics for separate filesets */
  void us_reset();
 
  /********************************************************************/
  /* The remainder of these functions are callback functions, intended
     only to be called by the main directory walker routines (walk and
     walk_impl).  Do not call these yourself. */

  /* walker is entering directory dirname: */
  void us_dir_enter(const char *dirname,const struct stat *s);

  /* walker is leaving directory dirname: */
  void us_dir_leave(const char *dirname,const struct stat *s);

  /* walker wants disk usage statistics updated for this file/dir: */
  void us_file_found(const char *filename,const struct stat *s);

  /* walker just deleted this file/dir (us_file_found will NOT be called): */
  void us_file_deleted(const char *filename,const struct stat *s);

  /* walker cannot call opendir on this dirname: */
  void us_dir_unopenable(const char *dirname,const struct stat *s);

  /* walker will not recurse because this dirname is too deeply nested: */
  void us_dir_too_deep(const char *filename,const struct stat *s);

  /* walker will not process a filename because it is too long
       dirname -- parent directory, whose path length is okay
       filepart -- file basename whose name is too long
       s -- stat structure for the DIRECTORY, not the file
  */
  void us_filename_too_long(const char *dirname,const char *filepart,const struct stat *s);

  /* walker will not process a filename because it would be too long
     when appended to the directory path
       dirname -- parent directory, whose path length is okay
       filepart -- file basename, whose length is okay, BUT
           the dirname+"/"+filepart would be too long
       s -- stat structure for the DIRECTORY not the file */
  void us_path_too_long(const char *dirname,const char *filepart,const struct stat *s);

  /* us_duplicate: called when a duplicate file is found.  Could be a
     hard link, or a mv executed during execution of this program.
     Note that this is not called for the first time the file is
     found, just the second and thereafter. */
  void us_dupicate(const char *filename,const struct stat *s);

#ifdef __cplusplus
}
#endif

#endif /* INC_DISK_USAGE */
