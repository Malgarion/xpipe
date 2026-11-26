CFLAGS ?= -O2 -g
HELP2MAN ?= help2man
XPIPE_VERSION ?= 0.1.0
.PHONY: man clean
xpipe: main.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -std=c11 -Wall -Wextra -Wpedantic -pthread $(LDFLAGS) -o $@ $< $(LDLIBS)
clean:
	$(RM) xpipe xpipe.1
man: xpipe
	$(HELP2MAN) --no-info --help-option=-h \
		--version-string="xpipe $(XPIPE_VERSION)" \
		--name="run a command in parallel on batches of input lines" \
		--output=xpipe.1 ./xpipe
