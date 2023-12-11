OBJS =  server.o serialization.o hashtable.o avl.o heap.o sortedSet.o threadPool.o
headers = header.h
CFLAGS= -std=c++11

main:  server.o serialization.o hashtable.o avl.o heap.o sortedSet.o threadPool.o header.h
	clang++ -o main $(OBJS) $(CFLAGS)
