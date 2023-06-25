CPU Affinity Test Program
=========================

This is a simple C program that prints the affinity of each thread and
process.  It can be compiled with or without MPI, and with or without
OpenMP.  OpenMP support is auto-detected via the `_OPENMP` variable;
MPI support must be enabled or disabled by specifying the Make
variable `USE_MPI=1` or `USE_MPI=0`.  CPU affinity, rank, thread
index, and hostname are printed for each thread of each MPI rank.  One
can debug affinity issues by parsing this information.

Requirements
------------

This program requires:

1. The Linux kernel compiled with cpuset support.  This can be
   determined by looking for `nodev cpuset` in the `/proc/filesystems`
   file.
2. The glibc library.
3. A C99-capable C compiler that uses the glibc library.

These requirements are met on most modern (ca. 2018) Linux clusters
and C compilers.

Compiling
---------

To compile:

    make CC=my_c_compiler CFLAGS=-flag-to-enable-openmp USE_MPI=1

Meanings of the options:

* `CC=my_c_compiler` specifies `my_c_compiler` as the name of the C compiler.
* `CFLAGS=-flag-to-enable-openmp` gives the list of C compilation
  flags.  This is where you specify OpenMP support.
* `USE_MPI=1` enables MPI support.  To disable, set `USE_MPI=0`.  The
  `USE_MPI` variable is mandatory.

Execution
---------

Execute this as you would any MPI, OpenMP, or serial program.  The
output will look like the following:

    rank 0000 thread 000 on host n001 is restricted to CPUs: 0 24
    rank 0000 thread 001 on host n001 is restricted to CPUs: 12 36
    rank 0001 thread 000 on host n001 is restricted to CPUs: 1 25
    rank 0001 thread 001 on host n001 is restricted to CPUs: 13 37
    rank 0002 thread 000 on host n002 is restricted to CPUs: 0 24
    rank 0002 thread 001 on host n002 is restricted to CPUs: 12 36
    rank 0003 thread 000 on host n002 is restricted to CPUs: 1 25
    rank 0003 thread 001 on host n002 is restricted to CPUs: 13 37
    rank 0004 thread 000 on host n100 is restricted to CPUs: 0 24
    rank 0004 thread 001 on host n100 is restricted to CPUs: 12 36
    rank 0005 thread 000 on host n100 is restricted to CPUs: 1 25
    rank 0005 thread 001 on host n100 is restricted to CPUs: 13 37

### Content:

* `rank 0005` is the MPI rank.  If MPI is disabled, this will always
  be `rank 0000`
* `thread 001` is the OpenMP thread index within each rank.  If OpenMP
  is disabled, this will always be `thread 000`.
* `host n100` specifies the hostname each rank resides on
* `CPUS: 0 24` lists the CPUs on which the thread is allowed to run.
  This is the CPU affinity information.

Implementation
--------------

This is how each piece of information is obtained:

* `rank 0005` comes from `MPI_Comm_rank()` run on `MPI_COMM_WORLD`
* `thread 000` comes from `omp_get_thread_num()` run within a parallel
  region that uses all available OpenMP threads.
* `host n100` comes from the `gethostname()` function.
* `CPUs: 1 25` comes from a multi-step process.  Internally, the Linux
  system call `gettid()` is used to get the Linux thread id.  This is
  the per-thread equivalent to the process ID.  The
  `sched_getaffinity()` provides the affinity information.  The number
  of CPUs in the cpu set is unknown, so the code loops all the way to
  the maxiumum possible number (`CPU_SETSIZE`) using `CPU_ISSET` to
  determine if the CPU is within the thread's CPU mask.