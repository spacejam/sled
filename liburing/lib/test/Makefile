CFLAGS ?= -g -O2
XCFLAGS =
override CFLAGS += -Wall -D_GNU_SOURCE -L../src/ -I../src/include/

all_targets += poll poll-cancel ring-leak fsync io_uring_setup io_uring_register \
	       io_uring_enter nop sq-full cq-full 35fa71a030ca-test \
		917257daa0fe-test b19062a56726-test eeed8b54e0df-test link \
		send_recvmsg a4c0b3decb33-test 500f9fbadef8-test timeout \
		sq-space_left stdout cq-ready cq-peek-batch file-register \
		cq-size 8a9973408177-test

include ../Makefile.quiet

all: $(all_targets)

%: %.c
	$(QUIET_CC)$(CC) $(CFLAGS) -o $@ $< -luring $(XCFLAGS)

test_srcs := poll.c poll-cancel.c ring-leak.c fsync.c io_uring_setup.c \
	io_uring_register.c io_uring_enter.c nop.c sq-full.c cq-full.c \
	35fa71a030ca-test.c 917257daa0fe-test.c b19062a56726-test.c \
	eeed8b54e0df-test.c link.c send_recvmsg.c a4c0b3decb33-test.c \
	500f9fbadef8-test.c timeout.c sq-space_left.c stdout.c cq-ready.c\
	cq-peek-batch.c file-register.c cq-size.c 8a9973408177-test.c

test_objs := $(patsubst %.c,%.ol,$(test_srcs))

35fa71a030ca-test: XCFLAGS = -lpthread

clean:
	rm -f $(all_targets) $(test_objs)

runtests:
	@./runtests.sh $(all_targets)
