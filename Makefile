CC     = gcc
CFLAGS = -g -lmpfr -lgmp 
#OPTFLAGS = -pg
# Only use -ftz if using threads
OPTFLAGS = #-ftz #-ipo #-O3
 
OMPFLAGS = -openmp
#CFLAGS = -g -lgmp

MPIFLAGS = -I/opt/openmpi/include -pthread -L/opt/openmpi/lib -lmpi -lopen-rte -lopen-pal -ldl -Wl,--export-dynamic -lnsl -lutil  

all: pthreads.ex

pthreads.ex: pthreads/pthreads.c
	$(CC) $(CFLAGS) $(OPTFLAGS) -o pthreads.ex pthreads/pthreads.c -lpthread

clean:
	rm -f *~ *.o 
	cd pthreads; rm -f *~ *.o 
