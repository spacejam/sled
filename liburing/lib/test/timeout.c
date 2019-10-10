/*
 * Description: run various timeout tests
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/time.h>

#include "liburing.h"

#define TIMEOUT_MSEC	1000
static int not_supported;

static unsigned long long mtime_since(const struct timeval *s,
				      const struct timeval *e)
{
	long long sec, usec;

	sec = e->tv_sec - s->tv_sec;
	usec = (e->tv_usec - s->tv_usec);
	if (sec > 0 && usec < 0) {
		sec--;
		usec += 1000000;
	}

	sec *= 1000;
	usec /= 1000;
	return sec + usec;
}

static unsigned long long mtime_since_now(struct timeval *tv)
{
	struct timeval end;

	gettimeofday(&end, NULL);
	return mtime_since(tv, &end);
}

/*
 * Test that we return to userspace if a timeout triggers, even if we
 * don't satisfy the number of events asked for.
 */
static int test_single_timeout_many(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	unsigned long long exp;
	struct __kernel_timespec ts;
	struct timeval tv;
	int ret;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	ts.tv_sec = TIMEOUT_MSEC / 1000;
	ts.tv_nsec = 0;
	io_uring_prep_timeout(sqe, &ts, 0);

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		printf("sqe submit failed: %d\n", ret);
		goto err;
	}

	gettimeofday(&tv, NULL);
	ret = io_uring_enter(ring->ring_fd, 0, 4, IORING_ENTER_GETEVENTS, NULL);
	if (ret < 0) {
		printf("io_uring_enter %d\n", ret);
		goto err;
	}

	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		printf("wait completion %d\n", ret);
		goto err;
	}
	if (cqe->res == -EINVAL) {
		printf("Timeout not supported, ignored\n");
		not_supported = 1;
		return 0;
	} else if (cqe->res != -ETIME) {
		printf("Timeout: %s\n", strerror(-cqe->res));
		goto err;
	}
	io_uring_cqe_seen(ring, cqe);

	exp = mtime_since_now(&tv);
	if (exp >= TIMEOUT_MSEC / 2 && exp <= (TIMEOUT_MSEC * 3) / 2)
		return 0;
	printf("Timeout seems wonky (got %llu)\n", exp);
err:
	return 1;
}

/*
 * Test numbered trigger of timeout
 */
static int test_single_timeout_nr(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct __kernel_timespec ts;
	int i, ret;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	ts.tv_sec = TIMEOUT_MSEC / 1000;
	ts.tv_nsec = 0;
	io_uring_prep_timeout(sqe, &ts, 2);

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	io_uring_sqe_set_data(sqe, (void *) 1);
	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	io_uring_sqe_set_data(sqe, (void *) 1);

	ret = io_uring_submit_and_wait(ring, 4);
	if (ret <= 0) {
		printf("sqe submit failed: %d\n", ret);
		goto err;
	}

	i = 0;
	while (i < 3) {
		ret = io_uring_wait_cqe(ring, &cqe);
		if (ret < 0) {
			printf("wait completion %d\n", ret);
			goto err;
		}

		/*
		 * NOP commands have user_data as 1. Check that we get the
		 * two NOPs first, then the successfully removed timout as
		 * the last one.
		 */
		switch (i) {
		case 0:
		case 1:
			if (io_uring_cqe_get_data(cqe) != (void *) 1) {
				printf("nop not seen as 1 or 2\n");
				goto err;
			}
			break;
		case 2:
			if (io_uring_cqe_get_data(cqe) != NULL) {
				printf("timeout not last\n");
				goto err;
			}
			break;
		}

		if (cqe->res < 0) {
			printf("Timeout: %s\n", strerror(-cqe->res));
			goto err;
		} else if (cqe->res) {
			printf("res: %d\n", cqe->res);
			goto err;
		}
		io_uring_cqe_seen(ring, cqe);
		i++;
	};

	return 0;
err:
	return 1;
}

static int test_single_timeout_wait(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	struct __kernel_timespec ts;
	int i, ret;

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	io_uring_sqe_set_data(sqe, (void *) 1);

	sqe = io_uring_get_sqe(ring);
	io_uring_prep_nop(sqe);
	io_uring_sqe_set_data(sqe, (void *) 1);

	ts.tv_sec = 1;
	ts.tv_nsec = 0;

	i = 0;
	do {
		ret = io_uring_wait_cqes(ring, &cqe, 2, &ts, NULL);
		if (ret == -ETIME)
			break;
		if (ret < 0) {
			printf("wait timeout failed: %d\n", ret);
			goto err;
		}

		if (cqe->res < 0) {
			printf("res: %d\n", cqe->res);
			goto err;
		}
		io_uring_cqe_seen(ring, cqe);
		i++;
	} while (1);

	if (i != 2) {
		printf("got %d completions\n", i);
		goto err;
	}
	return 0;
err:
	return 1;
}

/*
 * Test single timeout waking us up
 */
static int test_single_timeout(struct io_uring *ring)
{
	struct io_uring_cqe *cqe;
	struct io_uring_sqe *sqe;
	unsigned long long exp;
	struct __kernel_timespec ts;
	struct timeval tv;
	int ret;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	ts.tv_sec = TIMEOUT_MSEC / 1000;
	ts.tv_nsec = 0;
	io_uring_prep_timeout(sqe, &ts, 0);

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		printf("sqe submit failed: %d\n", ret);
		goto err;
	}

	gettimeofday(&tv, NULL);
	ret = io_uring_wait_cqe(ring, &cqe);
	if (ret < 0) {
		printf("wait completion %d\n", ret);
		goto err;
	}
	if (cqe->res == -EINVAL) {
		printf("Timeout not supported, ignored\n");
		not_supported = 1;
		return 0;
	} else if (cqe->res != -ETIME) {
		printf("Timeout: %s\n", strerror(-cqe->res));
		goto err;
	}
	io_uring_cqe_seen(ring, cqe);

	exp = mtime_since_now(&tv);
	if (exp >= TIMEOUT_MSEC / 2 && exp <= (TIMEOUT_MSEC * 3) / 2)
		return 0;
	printf("Timeout seems wonky (got %llu)\n", exp);
err:
	return 1;
}

/*
 * Test that timeout is canceled on exit
 */
static int test_single_timeout_exit(struct io_uring *ring)
{
	struct io_uring_sqe *sqe;
	struct __kernel_timespec ts;
	int ret;

	sqe = io_uring_get_sqe(ring);
	if (!sqe) {
		printf("get sqe failed\n");
		goto err;
	}

	ts.tv_sec = 30;
	ts.tv_nsec = 0;
	io_uring_prep_timeout(sqe, &ts, 0);

	ret = io_uring_submit(ring);
	if (ret <= 0) {
		printf("sqe submit failed: %d\n", ret);
		goto err;
	}

	io_uring_queue_exit(ring);
	return 0;
err:
	io_uring_queue_exit(ring);
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

	ret = test_single_timeout(&ring);
	if (ret) {
		printf("test_single_timeout failed\n");
		return ret;
	}
	if (not_supported)
		return 0;

	ret = test_single_timeout_many(&ring);
	if (ret) {
		printf("test_single_timeout_many failed\n");
		return ret;
	}

	ret = test_single_timeout_nr(&ring);
	if (ret) {
		printf("test_single_timeout_nr failed\n");
		return ret;
	}

	ret = test_single_timeout_wait(&ring);
	if (ret) {
		printf("test_single_timeout_wait failed\n");
		return ret;
	}

	/*
	 * this test must go last, it kills the ring
	 */
	ret = test_single_timeout_exit(&ring);
	if (ret) {
		printf("test_single_timeout_nr failed\n");
		return ret;
	}

	return 0;
}
