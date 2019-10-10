/*
 * Description: check that STDOUT write works
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "liburing.h"

static int test_pipe_io(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct iovec vecs;
	int ret;

	vecs.iov_base = "This is a pipe test\n";
	vecs.iov_len = strlen(vecs.iov_base);

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}
	io_uring_prep_writev(sqe, STDOUT_FILENO, &vecs, 1, 0);

	ret = io_uring_submit(ring);
	if (ret < 1) {
		printf("Submitted only %d\n", ret);
		goto err;
	} else if (ret < 0) {
		printf("sqe submit failed: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		printf("wait completion %d\n", ret);
		goto err;
	}
	if (cqe->res < 0) {
		printf("STDOUT write error: %s\n", strerror(-cqe->res));
		goto err;
	}
	if (cqe->res != vecs.iov_len) {
		printf("Got %d write, wanted %d\n", cqe->res, (int)vecs.iov_len);
		goto err;
	}
	io_uring_cqe_seen(ring, cqe);

	return 0;
err:
	return 1;
}

int main(int argc, char *argv[])
{
	struct io_uring ring;
	int ret;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		printf("ring setup failed\n");
		return 1;

	}

	ret = test_pipe_io(&ring);
	if (ret) {
		printf("test_pipe_io failed\n");
		return ret;
	}

	return 0;
}
