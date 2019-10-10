/*
 * Description: run various linked sqe tests
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "liburing.h"

/*
 * Test failing head of chain, and dependent getting -ECANCELED
 */
static int test_single_link_fail(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret, i;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_nop(sqe);
	sqe->flags |= IOSQE_IO_LINK;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_nop(sqe);

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		printf("sqe submit failed: %d\n", ret);
		goto err;
	}

	for (i = 0; i < 2; i++) {
		ret = io_uring_peek_cqe(ring, &cqe);
		if (ret < 0) {
			printf("wait completion %d\n", ret);
			goto err;
		}
		if (!cqe) {
			printf("failed to get cqe\n");
			goto err;
		}
		if (i == 0 && cqe->res != -EINVAL) {
			printf("sqe0 failed with %d, wanted -EINVAL\n", cqe->res);
			goto err;
		}
		if (i == 1 && cqe->res != -ECANCELED) {
			printf("sqe0 failed with %d, wanted -ECANCELED\n", cqe->res);
			goto err;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	return 0;
err:
	return 1;
}

/*
 * Test two independent chains
 */
static int test_double_chain(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret, i;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_nop(sqe);
	sqe->flags |= IOSQE_IO_LINK;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_nop(sqe);

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_nop(sqe);
	sqe->flags |= IOSQE_IO_LINK;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_nop(sqe);

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		printf("sqe submit failed: %d\n", ret);
		goto err;
	}

	for (i = 0; i < 4; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret < 0) {
			printf("wait completion %d\n", ret);
			goto err;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	return 0;
err:
	return 1;
}

/*
 * Test multiple dependents
 */
static int test_double_link(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret, i;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_nop(sqe);
	sqe->flags |= IOSQE_IO_LINK;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_nop(sqe);
	sqe->flags |= IOSQE_IO_LINK;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_nop(sqe);

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		printf("sqe submit failed: %d\n", ret);
		goto err;
	}

	for (i = 0; i < 3; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret < 0) {
			printf("wait completion %d\n", ret);
			goto err;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	return 0;
err:
	return 1;
}

/*
 * Test single dependency
 */
static int test_single_link(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	int ret, i;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_nop(sqe);
	sqe->flags |= IOSQE_IO_LINK;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	io_uring_prep_nop(sqe);

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		printf("sqe submit failed: %d\n", ret);
		goto err;
	}

	for (i = 0; i < 2; i++) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret < 0) {
			printf("wait completion %d\n", ret);
			goto err;
		}
		io_uring_cqe_seen(ring, cqe);
	}

	return 0;
err:
	return 1;
}

int main(int argc, char *argv[])
{
	struct io_uring ring, poll_ring;
	int ret;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		printf("ring setup failed\n");
		return 1;

	}

	ret = io_uring_queue_init(8, &poll_ring, IORING_SETUP_IOPOLL);
	if (ret) {
		printf("poll_ring setup failed\n");
		return 1;

	}

	ret = test_single_link(&ring);
	if (ret) {
		printf("test_single_link failed\n");
		return ret;
	}

	ret = test_double_link(&ring);
	if (ret) {
		printf("test_double_link failed\n");
		return ret;
	}

	ret = test_double_chain(&ring);
	if (ret) {
		printf("test_double_chain failed\n");
		return ret;
	}

	ret = test_single_link_fail(&poll_ring);
	if (ret) {
		printf("test_single_link_fail failed\n");
		return ret;
	}

	return 0;
}
