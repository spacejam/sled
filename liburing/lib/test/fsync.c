/*
 * Description: test io_uring fsync handling
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "liburing.h"

static int test_single_fsync(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	char buf[32];
	int fd, ret;

	sprintf(buf, "./XXXXXX");
	fd = mkstemp(buf);
	if (fd < 0) {
		perror("open");
		return 1;
	}

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_fsync(sqe, fd, 0);

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		printf("sqe submit failed: %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		printf("wait completion %d\n", ret);
		goto err;
	}

	io_uring_cqe_seen(ring, cqe);
	unlink(buf);
	return 0;
err:
	unlink(buf);
	return 1;
}

static int test_barrier_fsync(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct iovec iovecs[4];
	int i, fd, ret;
	off_t off;

	fd = open("testfile", O_WRONLY | O_CREAT, 0644);
	if (fd < 0) {
		perror("open");
		return 1;
	}

	for (i = 0; i < 4; i++) {
		iovecs[i].iov_base = malloc(4096);
		iovecs[i].iov_len = 4096;
	}

	off = 0;
	for (i = 0; i < 4; i++) {
		sqe = io_uring_get_sqe(ring);
		if (!sqe) {
			printf("get sqe failed\n");
			goto err;
		}

		io_uring_prep_writev(sqe, fd, &iovecs[i], 1, off);
		sqe->user_data = 0;
		off += 4096;
	}

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
	sqe->user_data = 1;
	io_uring_sqe_set_flags(sqe, IOSQE_IO_DRAIN);

	ret = io_uring_submit(ring);
	if (ret < 5) {
		printf("Submitted only %d\n", ret);
		goto err;
	} else if (ret < 0) {
		printf("sqe submit failed\n");
		if (ret == EINVAL)
			printf("kernel may not support barrier fsync yet\n");
		goto err;
	}

	for (i = 0; i < 5; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret < 0) {
			printf("child: wait completion %d\n", ret);
			goto err;
		}
		/* kernel doesn't support IOSQE_IO_DRAIN */
		if (cqe->res == -EINVAL)
			break;
		if (i <= 3) {
			if (cqe->user_data) {
				printf("Got fsync early?\n");
				goto err;
			}
		} else {
			if (!cqe->user_data) {
				printf("Got write late?\n");
				goto err;
			}
		}
		io_uring_cqe_seen(ring, cqe);
	}

	unlink("testfile");
	return 0;
err:
	unlink("testfile");
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

	ret = test_single_fsync(&ring);
	if (ret) {
		printf("test_single_fsync failed\n");
		return ret;
	}

	ret = test_barrier_fsync(&ring);
	if (ret) {
		printf("test_barrier_fsync failed\n");
		return ret;
	}

	return 0;
}
