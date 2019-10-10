CFLAGS ?= -g -O2
XCFLAGS =
override CFLAGS += -Wall -D_GNU_SOURCE -L../src/ -I../src/include/

include ../Makefile.quiet

all_targets += io_uring-test io_uring-cp link-cp ucontext-cp

all: $(all_targets)

test_srcs := io_uring-test.c io_uring-cp.c link-cp.c

test_objs := $(patsubst %.c,%.ol,$(test_srcs))

%: %.c
	$(QUIET_CC)$(CC) $(CFLAGS) -o $@ $< -luring $(XCFLAGS)

clean:
	rm -f $(all_targets) $(test_objs)
