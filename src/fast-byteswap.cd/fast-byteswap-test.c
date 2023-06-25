#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdarg.h>
#include <sys/time.h>
#include <sys/resource.h>

#include "fast-byteswap.h"

void warn(const char *format,...) {
  va_list ap;
  va_start(ap,format);
  vfprintf(stderr,format,ap);
  va_end(ap);
}
void die(const char *format,...) {
  va_list ap;
  va_start(ap,format);
  vfprintf(stderr,format,ap);
  va_end(ap);
  abort();
  /* Should never get here */
  exit(2);
}

void realloc_it(off_t *asize,void **abuf,void **buf,off_t newsize) {
  /* Extend buffer from *asize bytes to newsize bytes, or allocate it
     if it has not yet been allocated.  Does nothing if the buffer is
     already large enough or larger.  */
  if(newsize+8<=*asize) return;
  if(*asize==0) {
    if( ! (*buf=malloc(*asize=newsize+8)) )
      die("cannot alloc %lld bytes: %s",(long long)newsize,strerror(errno));
  } else {
    if( ! (*buf=realloc(*buf,*asize=newsize+8)) )
      die("cannot realloc %lld bytes: %s",(long long)newsize,strerror(errno));
  }
  *abuf=(void*)( ((size_t)(*buf)+7) / 8 * 8 );
}

void update_usage(double *accum,double *val,struct timeval *tv1,struct timeval *tv2) {
  *val=(tv2->tv_sec-tv1->tv_sec) + 
    1e-6* ( ((double)tv2->tv_usec) - ((double)tv1->tv_usec));
  *accum+=*val;
}

const char *find_basename(const char *argv0) {
  const char *there=argv0 + (strlen(argv0)-1);
  while(there>argv0 && *there!='/') there--;
  if(*there=='/') there++;
  return there;
}

void usage(const char * argv0,const char *error) {
  warn("Usage: %s swapsize file [file [file [...] ] ]\n"
       "byteswaps files in-place.  The swapsize is the size of the fields to swap.\n"
       "swapsize = 16, 32 or 64 for 16-bit, 32-bit and 64-bit fields.\n%s",find_basename(argv0),error);
  exit(2);
}

int main(int argc,char **argv) {
  int argi;
  FILE *f;
  struct stat statbuf;
  void *xbuffer=NULL,*buffer=NULL; /* xbuffer is return from malloc/realloc, buffer is 8-byte aligned */
  off_t bufsize=0;
  int swapsize=-1,tries=0,swapbytes;

  unsigned long long bytes=0,mybytes;
  const unsigned long long gb=1<<30;
  struct rusage usage1,usage2;
  double usertime=0,systime=0,walltime=0;
  double user1,sys1,wall1;
  struct timeval tod1,tod2;

  if(argc<3)
    usage(argv[0],"provide at least two arguments\n");

  swapsize=atoi(argv[1]);
  if(swapsize!=16 && swapsize!=32 && swapsize!=64)
    usage(argv[0],"invalid swapsize: must be 16, 32 or 64\n");
  swapbytes=swapsize/8;

  for(argi=2;argi<argc;argi++) {
    printf("%s: read...\n",argv[argi]);
    /* Open for read+write and get size */
    if(! (f=fopen(argv[argi],"rb+")) )
      die("%s: cannot open for read+write: %s\n",argv[argi],strerror(errno));
    if(fstat(fileno(f),&statbuf)) {
      fclose(f);
      die("%s: cannot stat after opening file: %s\n",argv[argi],strerror(errno));
    }

    /* Check the size (should be multiple of four bytes) */
    if(statbuf.st_size<=0) {
      /* Nothing to do: file is empty. */
      fclose(f);
      continue;
    }
    if(statbuf.st_size/4*4 != statbuf.st_size)
      die("%s: file is not a multiple of four bytes (size %lld)\n",
          argv[argi],(long long)statbuf.st_size);

    /* Allocate memory */
    realloc_it(&bufsize,&buffer,&xbuffer,statbuf.st_size);

    /* Read data */
    if(1!=fread(buffer,statbuf.st_size,1,f))
      die("%s: cannot read full file (%d bytes): %s\n",
          argv[argi],(long long)statbuf.st_size,strerror(errno));

    printf("%s: byteswap and time...\n",argv[argi]);

    /* Get resource usage and time before swapping data */
    if(getrusage(RUSAGE_SELF,&usage1))
      die("error getting resource usage: %s\n",strerror(errno));
    if(gettimeofday(&tod1,NULL))
      die("error getting time of day (gettimeofday): %s\n",strerror(errno));

    /* Swap data */
    bytes+=mybytes=statbuf.st_size;
    if(!fast_byteswap(buffer,swapbytes,statbuf.st_size/swapbytes))
      die("%s: cannot byteswap\n",argv[argi]);

    /* Get resource usage and time after swapping data */
    if(getrusage(RUSAGE_SELF,&usage2))
      die("error getting resource usage: %s\n",strerror(errno));
    if(gettimeofday(&tod2,NULL))
      die("error getting time of day (gettimeofday): %s\n",strerror(errno));

    /* Accumulate user, sys, wall time */
    update_usage(&usertime,&user1,&usage1.ru_utime,&usage2.ru_utime);
    update_usage(&systime,&sys1,&usage1.ru_stime,&usage2.ru_stime);
    update_usage(&walltime,&wall1,&tod1,&tod2);
    
    printf("%s: this file: %llu bytes: real=%fs (%fgb/s) user=%fs (%fgb/s) sys=%fs\n",
           argv[argi],mybytes,wall1,mybytes/wall1/gb,user1,mybytes/user1/gb,sys1);
    printf("%s: total so far: %llu bytes: real=%fs (%fgb/s) user=%fs (%fgb/s) sys=%fs\n",
           argv[argi],bytes,walltime,bytes/walltime/gb,usertime,bytes/usertime/gb,systime);
    
    printf("%s: write...\n",argv[argi]);

    /* Seek back to beginning of the file and rewrite data */
    if(fseek(f,SEEK_SET,0))
      die("%s: error seeking to beginning of file: %s\n",argv[argi],strerror(errno));
    if(1!=fwrite(buffer,statbuf.st_size,1,f))
      die("%s: error writing file (%d bytes): %s\n",
          argv[argi],(long long)statbuf.st_size,strerror(errno));
    if(fclose(f))
      warn("%s: warning: error closing file; %s\n",argv[argi],strerror(errno));
    printf("%s: done.\n",argv[argi]);
  }

  printf("Time used for byte swapping %llu bytes:\nreal\t%fs\t(%f gb/s)\nuser\t%fs\t(%f gb/s)\nsys\t%fs\t(%f gb/s)\n",
         bytes,walltime,bytes/walltime/gb,usertime,bytes/usertime/gb,systime,bytes/systime/gb);
  return 0;
}
