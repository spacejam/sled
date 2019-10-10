/*
 * Description: test io_uring poll handling
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/poll.h>
#include <sys/wait.h>

#include "liburing.h"

static void sig_alrm(int sig)
{
	printf("Timed out!\n");
	exit(1);
}

int main(int argc, char *argv[])
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct io_uring ring;
	int pipe1[2];
	pid_t p;
	int ret;

	if (pipe(pipe1) != 0) {
		printf("pipe failed\n");
		return 1;
	}

	p = fork();
	switch (p) {
	case -1:
		printf("fork failed\n");
		exit(2);
	case 0: {
		struct sigaction act;

		ret = io_uring_queue_init(1, &ring, 0);
		if (ret) {
			printf("child: ring setup failed\n");
			return 1;
		}

		memset(&act, 0, sizeof(act));
		act.sa_handler = sig_alrm;
		act.sa_flags = SA_RESTART;
		sigaction(SIGALRM, &act, NULL);
		alarm(1);

		sqe = io_uring_get_sqe(&ring);
		if (!sqe) {
			printf("child: get sqe failed\n");
			return 1;
		}

		io_uring_prep_poll_add(sqe, pipe1[0], POLLIN);
		io_uring_sqe_set_data(sqe, sqe);

		ret = io_uring_submit(&ring);
		if (ret <= 0) {
			printf("child: sqe submit failed\n");
			return 1;
		}

		do {
			ret = io_uring_wait_cqe(&ring, &cqe);
			if (ret < 0) {
				printf("child: wait completion %d\n", ret);
				break;
			}
			io_uring_cqe_seen(&ring, cqe);
		} while (ret != 0);

		if (ret < 0) {
			printf("child: completion get failed\n");
			return 1;
		}
		if (cqe->user_data != (unsigned long) sqe) {
			printf("child: cqe doesn't match sqe\n");
			return 1;
		}
		if ((cqe->res & POLLIN) != POLLIN) {
			printf("child: bad return value %ld\n", (long) cqe->res);
			return 1;
		}
		exit(0);
		}
	default:
		do {
			errno = 0;
			ret = write(pipe1[1], "foo", 3);
		} while (ret == -1 && errno == EINTR);

		if (ret != 3) {
			printf("parent: bad write return %d\n", ret);
			return 1;
		}
		return 0;
	}
}
