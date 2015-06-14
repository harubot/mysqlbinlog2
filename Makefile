CC = g++
CFLAGS = -g -Wall -lPocoFoundation
OBJS = main.o mysqlbinlog.o
TARGET = mysqlbinlog2

%.o: %.cpp
	$(CC) $(CFLAGS) -o $@ -c $<

ALL: $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS)

mysqlbinlog.o: mysqlbinlog.h

clean:
	rm -rf $(OBJS) $(TARGET)
