CC=gcc
CFLAGS=-O3

PLATFORM = $(shell uname)
ifeq ($(PLATFORM),Darwin)
	SYSTEM_LIBS := -lpthread
endif
ifeq ($(PLATFORM),Linux)
	SYSTEM_LIBS := -lpthread -lzmq -ldl -lmsgpackc
endif

all: sqliteserver

sqliteserver: main.c
	$(CC) $(CFLAGS) dbresult.c dbworker.c sqlite3.c main.c -Iinclude -I/usr/local/include $(SYSTEM_LIBS) -o sqliteserver

clean:
	rm sqliteserver