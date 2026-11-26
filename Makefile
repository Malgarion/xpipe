CFLAGS ?= -O2 -g
.PHONY: all clean
all: xpipe
xpipe: main.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -std=c11 -Wall -Wextra -Wpedantic -pthread $(LDFLAGS) -o $@ $< $(LDLIBS)
clean:
	$(RM) xpipe
