HADOOP_HOME=/usr/local/hadoop
CPPFLAGS= -I$(HADOOP_HOME)/src/c++/libhdfs -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux  -I ../
LIB = -L$(HADOOP_HOME)/c++/Linux-amd64-64/lib
LDFLAGS = -lhdfs -Wno-deprecated -O2

main:
	g++ $(CPPFLAGS) $(LIB) $(LDFLAGS) -std=c++11 main.cpp -o main

clean:
	rm main
