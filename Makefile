CC     = gcc
CFLAGS = -g 
# Only use -ftz if using threads
OPTFLAGS = #-ftz #-ipo #-O3 
OMPFLAGS = -openmp
MPIFLAGS = -I/opt/openmpi/include -pthread -L/opt/openmpi/lib -lmpi -lopen-rte -lopen-pal -ldl -Wl,--export-dynamic -lnsl -lutil  
LINK = -L/usr/lib/x86_64-linux-gnu/libmpfr.a -lmpfr -L/usr/lib/x86_64-linux-gnu/libgmp.a -lgmp -lpthread -lm
JAVAC = javac


all: pthreads.ex

pthreads.ex: pthreads/pthreads.c
	$(CC) $(CFLAGS) $(OPTFLAGS) -o pthreads.ex pthreads/pthreads.c $(LINK)

clean:
	rm -f *~ *.o 
	cd pthreads; rm -f *~ *.o 
