SHELL=		/bin/sh
SRCS=		mpiserial.c
OBJS=		mpiserial.o
LSTS=		mpiserial.lst
CMD=		mpiserial

# IFORT WITH SGI MPT (Zeus):
#CC=		mpicc
#CFLAGS=		-DMPI_ABORT_FOR_NONZERO_EXIT

# IFORT OR XLC WITH IBM POE (Tide, Gyre)
CC=		mpcc
CFLAGS=		

# IFORT WITH INTEL MPI (Jet):
#CC=		mpicc
#CFLAGS=		

all:		$(CMD)

$(CMD):		$(OBJS) makefile
	$(CC) $(CFLAGS) -o $(CMD) $(OBJS)

mpiserial.o:	mpiserial.c makefile
	$(CC) -c $(CFLAGS) mpiserial.c

clean:
	rm -f $(OBJS) xref.db *~ *.o *.a a.out cmdfile *.out *.err *.log

bare: clean
	rm -f $(CMD)