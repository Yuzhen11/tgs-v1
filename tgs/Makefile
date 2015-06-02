CCOMPILE=mpic++
PLATFORM=Linux-amd64-64

HADOOP_HOME=/usr/local/hadoop

CPPFLAGS= -I$(HADOOP_HOME)/src/c++/libhdfs -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux  -I../
LIB = -L$(HADOOP_HOME)/c++/$(PLATFORM)/lib
LDFLAGS = -lhdfs -Wno-deprecated -O2

all: run

run: tgs.cpp
	$(CCOMPILE) -std=c++11 tgs.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS)  -o tgs
	./kill.py
	
clean:
	-rm run
