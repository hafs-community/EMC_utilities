ifneq ($(USE_MPI),1)
  ifneq ($(USE_MPI),0)
    $(error Set USE_MPI to 1 or 0)
  endif
endif

CPPFLAGS+=-DUSE_MPI=$(USE_MPI)

cputest: cputest.c