/*
 * Description: test SQ queue space left
 *
 */
#include <errno.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

#include "liburing.h"

int main(int argc, char *argv[])
{
	struct io_uring_sqe *sqe;
	struct io_uring ring;
	int ret, i = 0, s;

	ret = io_uring_queue_init(8, &ring, 0);
	if (ret) {
		printf("ring setup failed\n");
		return 1;

	}

	if ((s = io_uring_sq_space_left(&ring)) != 8) {
		printf("Got %d SQEs left, expected %d\n", s, 8);
		goto err;
	}

	i = 0;
	while ((sqe = io_uring_get_sqe(&ring)) != NULL) {
		i++;
		if ((s = io_uring_sq_space_left(&ring)) != 8 - i) {
			printf("Got %d SQEs left, expected %d\n", s, 8 - i);
			goto err;
		}
	}

	if (i != 8) {
		printf("Got %d SQEs, expected %d\n", i, 8);
		goto err;
	}

	io_uring_queue_exit(&ring);
	return 0;
err:
	io_uring_queue_exit(&ring);
	return 1;
}
