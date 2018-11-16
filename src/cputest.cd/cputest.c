/* Portability note: this uses Linux-specific and glibc-specific
   features.  It will only work on modern GNU Linux. */

#define _GNU_SOURCE

////////////////////////////////////////////////////////////////////////

// Decide whether OpenMP and MPI are enabled.

#ifndef USE_MPI
#  warning USE_MPI is unset
#  define USE_MPI 0
#endif

// If user does not specify USE_OPENMP then detect OpenMP availability.

#ifndef USE_OPENMP
#  ifdef _OPENMP
#    define USE_OPENMP 1
#  else
#    define USE_OPENMP 0
#  endif
#endif

////////////////////////////////////////////////////////////////////////

// Standard and proprietary headers.  Note _GNU_SOURCE.

#include <sched.h>
#include <sys/types.h>
#include <sys/syscall.h>

#include <unistd.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#if USE_MPI
#  include <mpi.h>
#else
#  warning MPI is disabled in this build
#endif

#if USE_OPENMP
#  include <omp.h>
#else
#  warning OpenMP is disabled in this build
#endif

////////////////////////////////////////////////////////////////////////

// Workaround for bug in some versions of glibc: CPU_COUNT undefined

#ifndef CPU_COUNT
static int
CPU_COUNT(cpu_set_t *set) {
  int i, count=0;
  for(i=0;i<CPU_SETSIZE;i++)
    count += !! CPU_ISSET(i, set);
  return count;
}
#endif

////////////////////////////////////////////////////////////////////////

int deallocate_affinity(int **thread_cpu_list,int *nthreads);
int find_affinity(int ***thread_cpu_list,int *nthreads);
int print_affinity();
int main(int argc,char**argv);

////////////////////////////////////////////////////////////////////////

int deallocate_affinity(int **thread_cpu_list,int *nthreads) {
  int **free_me;
  if(thread_cpu_list && nthreads && *nthreads>0) {
    for(free_me=thread_cpu_list;free_me<thread_cpu_list+*nthreads;free_me++)
      if(*free_me)
        free(*free_me);
    free(thread_cpu_list);
  }
  return 0;
}

////////////////////////////////////////////////////////////////////////

int find_affinity(int ***thread_cpu_list_ptr,int *nthreads_ptr) {
  cpu_set_t mask;
  pid_t tid;
  int **thread_cpu_list; // to avoid too many *'s
  int nthreads;
  int ithread, icpu, fail;

#if USE_OPENMP
  #pragma omp parallel
  {
    nthreads=omp_get_num_threads();
  }
#else
  nthreads=1;
#endif
  nthreads = (nthreads<1) ? 1 : nthreads;

  *nthreads_ptr=nthreads;

  if( ! (thread_cpu_list=(int**)malloc(sizeof(int*) * nthreads)) )
    return -1;
  *thread_cpu_list_ptr = thread_cpu_list;

  memset(thread_cpu_list,0,sizeof(int*) * nthreads);

  fail=0;
#if USE_OPENMP
#pragma omp parallel private(ithread) reduction(+:fail) private(tid,mask,icpu)
  {
    ithread=omp_get_thread_num();
#else
    ithread=0;
#endif

    fail=0;

    tid=syscall(SYS_gettid);  // Linux thread ID
    
    /* Get the list of CPUs this thread can access.  Store -1 for each
       CPU that cannot be used, and the CPU number for CPUs that can
       be used. */

#if USE_OPENMP
#pragma omp critical
    {
#endif

      fail=sched_getaffinity(tid,sizeof(cpu_set_t),&mask);
      if(fail) {
        fprintf(stderr,"Cannot get scheduler affinity (return status %d): %s\n",fail,strerror(errno));
      }
      if(fail!=0) fail=1;

#if USE_OPENMP
    } // end of "omp critical"
#endif

    if(!fail) {
      thread_cpu_list[ithread] = (int*) malloc(sizeof(int)*CPU_SETSIZE);
      if(thread_cpu_list[ithread]) {
        for(icpu=0;icpu<CPU_SETSIZE;icpu++)
          thread_cpu_list[ithread][icpu] =
            (CPU_ISSET(icpu,&mask)) ? icpu : -1;
      } else {
        fprintf(stderr,"Cannot allocate memory: %s\n",strerror(errno));
        fail=1;
      }
    }

#if USE_OPENMP
  } // end of "omp parallel"
#endif
  
  return fail;
}

////////////////////////////////////////////////////////////////////////

int print_affinity() {
  int **thread_cpu_list=NULL, nthreads=0, rank=-1, ithread, icpu;
  int nranks=0, naffinity;
  char hostname[HOST_NAME_MAX+1];
  char *buf=NULL;
  size_t bufsize=0, bufneed=0, strend;

#if USE_MPI
  if(MPI_Comm_rank(MPI_COMM_WORLD,&rank)) {
    fprintf(stderr,"Cannot determine MPI rank.\n");
    return -1;
  }
  if(MPI_Comm_size(MPI_COMM_WORLD,&nranks)) {
    fprintf(stderr,"Cannot determine MPI_COMM_WORLD size.\n");
    return -1;
  }
#endif

  if(rank<0) rank=0;
  if(nranks<1) nranks=1;

  /* if(rank==0) */
  /*   printf("nranks=%d\n",nranks); */

  if(find_affinity(&thread_cpu_list,&nthreads)) {
    fprintf(stderr,"Cannot get thread CPU affinity: %s\n",strerror(errno));
    return -2;
  }

  memset(hostname,0,HOST_NAME_MAX+1);
  if(gethostname(hostname,HOST_NAME_MAX+1)) {
    fprintf(stderr,"Cannot get hostname: %s\n",strerror(errno));
    return -3;
  }

  // printf("host=%s rank=%d nthreads=%d\n",hostname,rank,nthreads);

  for(ithread=0;ithread<nthreads;ithread++) {
    bufneed=HOST_NAME_MAX + 500 + CPU_SETSIZE*5;
    if(bufsize<bufneed) {

      if(buf && bufsize)
        buf=(char*)realloc(buf,bufneed);
      else
        buf=(char*)malloc(bufneed);
      if(!buf) {
        fprintf(stderr,"Cannot allocate %llu bytes: %s\n",
                (unsigned long long)bufneed,strerror(errno));
        return -4;
      }
      bufsize=bufneed;
    } // allocate buffer

    if(0>=snprintf(buf,bufsize,"rank %04d thread %03d on host %s is restricted to CPUs:",
                   rank,ithread,hostname)) {
      fprintf(stderr,"Cannot write to internal array via snprintf: %s\n",
              strerror(errno));
      return -5;
    }
  
    strend=strlen(buf);

    naffinity=0;
    for(icpu=0;icpu<CPU_SETSIZE;icpu++) {
      if(thread_cpu_list[ithread][icpu]>=0) {
        if(0>=snprintf(buf+strend,bufsize-strend," %d",icpu)) {
          fprintf(stderr,"Cannot write to internal array via snprintf: %s\n",
                  strerror(errno));
          return -6;
        } else {
          strend += strlen(buf+strend);
          naffinity++;
        }
      } // at least one cpu is known
    } // thread cpu affinity loop

    if(naffinity<1) {
      fprintf(stderr,"Thread %d has no affinity information\n",ithread);
      return -11;
    }

    if(0>=printf("%s\n",buf)) {
      fprintf(stderr,"Cannot write to stdout: %s\n",strerror(errno));
      return -7;
    }
  } // thread loop
  return 0;
}

////////////////////////////////////////////////////////////////////////

int main(int argc, char **argv) {
#if USE_MPI
  if(MPI_Init(&argc,&argv)) {
    fprintf(stderr,"MPI_Init failed.\n");
    return 2;
  }
#endif

  if(print_affinity()) {
    fprintf(stderr,"Trouble printing affinity.\n");
    return 2;
  }

#if USE_MPI
  if(MPI_Finalize()) {
    fprintf(stderr,"MPI_Finalize failed.\n");
    return 2;
  }
#endif

  return 0;
}
