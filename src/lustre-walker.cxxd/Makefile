CC=gcc
CXX=g++
#CFLAGS=-Wall -W -O0 -g3 -I. -Wno-deprecated -std=c99
#CXXFLAGS=-Wall -W -O0 -g3 -I. -Wno-deprecated
CFLAGS=-Wall -W -O3 -I. -Wno-deprecated -std=c99
CXXFLAGS=-Wall -W -O3 -I. -Wno-deprecated
LIBS=-lacl

OBJS=main.o disk_usage.o check_dup.o basic_utils.o paranoia.o
EXE=../../bin/lustre-walker

all: $(EXE)
clean:
	rm -f *.o *~ \#*\# $(OBJS) core core.[0-9]*
bare: clean
	rm -f $(EXE)

paranoia.o: paranoia.c Makefile
main.o: main.c Makefile
basic_utils.o: basic_utils.c Makefile

disk_usage.o: disk_usage.c++ Makefile
	$(CXX) $(CXXFLAGS) -c -o $@ $<

check_dup.o: check_dup.c++ Makefile
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(EXE): $(OBJS)
	$(CXX) $(CXXFLAGS) $(LIBS) -o $(EXE) $(OBJS)


