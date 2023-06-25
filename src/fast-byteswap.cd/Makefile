CC=icc
CFLAGS=-O3 -xhost -Wall

#CC=gcc
#CFLAGS=-O3 -Wall

#CC=pgcc
#CFLAGS=-O3

#CC=xlc
#CFLAGS=-O5

EXE=../../bin/fast-byteswap

all: $(EXE)

$(EXE): fast-byteswap-test.o fast-byteswap.o Makefile
	$(CC) -o $(EXE) fast-byteswap-test.o fast-byteswap.o

bare: clean
	rm -f $(EXE)

clean:
	rm -f *.o *~ \#*