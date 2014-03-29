CC=gcc
CFLAGS=`pkg-config --cflags opencv`
LIBS=`pkg-config --libs opencv`


all: FaceDetection.o
	$(CC) -o FaceDetection.out $^ $(LIBS)

%.o: %.cpp
	$(CC) -c -o $@ $< $(CFLAGS)

.PHONY: clean
clean:
	rm -f *.o FaceDetection
