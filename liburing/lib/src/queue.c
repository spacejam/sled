#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdbool.h>

#include "liburing/compat.h"
#include "liburing/io_uring.h"
#include "liburing.h"
#include "liburing/barrier.h"

int __io_uring_get_cqe(struct io_uring *ring, struct io_uring_cqe **cqe_ptr,
		       unsigned submit, unsigned wait_nr, sigset_t *sigmask)
{
	int ret, err = 0;

	do {
		err = __io_uring_peek_cqe(ring, cqe_ptr);
		if (err || *cqe_ptr)
			break;
		if (!wait_nr) {
			err = -EAGAIN;
			break;
		}
		ret = io_uring_enter(ring->ring_fd, submit, wait_nr,
					IORING_ENTER_GETEVENTS, sigmask);
		if (ret < 0)
			err = -errno;
		submit -= ret;
	} while (!err);

	return err;
}

/*
 * Fill in an array of IO completions up to count, if any are available.
 * Returns the amount of IO completions filled.
 */
unsigned io_uring_peek_batch_cqe(struct io_uring *ring,
				 struct io_uring_cqe **cqes, unsigned count)
{
	unsigned ready;

	ready = io_uring_cq_ready(ring);
	if (ready) {
		unsigned head = *ring->cq.khead;
		unsigned mask = *ring->cq.kring_mask;
		unsigned last;
		int i = 0;

		count = count > ready ? ready : count;
		last = head + count;
		for (;head != last; head++, i++)
			cqes[i] = &ring->cq.cqes[head & mask];

		return count;
	}

	return 0;
}

/*
 * Sync internal state with kernel ring state on the SQ side
 */
static int __io_uring_flush_sq(struct io_uring *ring)
{
	struct io_uring_sq *sq = &ring->sq;
	const unsigned mask = *sq->kring_mask;
	unsigned ktail, submitted, to_submit;

	if (sq->sqe_head == sq->sqe_tail)
		return 0;

	/*
	 * Fill in sqes that we have queued up, adding them to the kernel ring
	 */
	submitted = 0;
	ktail = *sq->ktail;
	to_submit = sq->sqe_tail - sq->sqe_head;
	while (to_submit--) {
		sq->array[ktail & mask] = sq->sqe_head & mask;
		ktail++;
		sq->sqe_head++;
		submitted++;
	}

	/*
	 * Ensure that the kernel sees the SQE updates before it sees the tail
	 * update.
	 */
	if (submitted)
		io_uring_smp_store_release(sq->ktail, ktail);

	return submitted;
}

/*
 * Like io_uring_wait_cqe(), except it accepts a timeout value as well. Note
 * that an sqe is used internally to handle the timeout. Applications using
 * this function must never set sqe->user_data to LIBURING_UDATA_TIMEOUT!
 *
 * Note that the application need not call io_uring_submit() before calling
 * this function, as we will do that on its behalf.
 */
int io_uring_wait_cqes(struct io_uring *ring, struct io_uring_cqe **cqe_ptr,
		       unsigned wait_nr, struct __kernel_timespec *ts,
		       sigset_t *sigmask)
{
	int ret;

	if (wait_nr && ts) {
		struct io_uring_sqe *sqe;

		/*
		 * If the SQ ring is full, we may need to submit IO first
		 */
		sqe = io_uring_get_sqe(ring);
		if (!sqe) {
			ret = io_uring_submit(ring);
			if (ret < 0)
				return ret;
			sqe = io_uring_get_sqe(ring);
		}
		io_uring_prep_timeout(sqe, ts, wait_nr);
		sqe->user_data = LIBURING_UDATA_TIMEOUT;
	}

	ret = __io_uring_flush_sq(ring);

	return __io_uring_get_cqe(ring, cqe_ptr, ret, wait_nr, sigmask);
}

/*
 * See io_uring_wait_cqes_timeout() - this function is the same, it just
 * always uses '1' as the wait_nr.
 */
int io_uring_wait_cqe_timeout(struct io_uring *ring,
			      struct io_uring_cqe **cqe_ptr,
			      struct __kernel_timespec *ts)
{
	return io_uring_wait_cqes(ring, cqe_ptr, 1, ts, NULL);
}

/*
 * Returns true if we're not using SQ thread (thus nobody submits but us)
 * or if IORING_SQ_NEED_WAKEUP is set, so submit thread must be explicitly
 * awakened. For the latter case, we set the thread wakeup flag.
 */
static inline bool sq_ring_needs_enter(struct io_uring *ring, unsigned *flags)
{
	if (!(ring->flags & IORING_SETUP_SQPOLL))
		return true;
	if ((*ring->sq.kflags & IORING_SQ_NEED_WAKEUP)) {
		*flags |= IORING_ENTER_SQ_WAKEUP;
		return true;
	}

	return false;
}

/*
 * Submit sqes acquired from io_uring_get_sqe() to the kernel.
 *
 * Returns number of sqes submitted
 */
static int __io_uring_submit(struct io_uring *ring, unsigned submitted,
			     unsigned wait_nr)
{
	unsigned flags;
	int ret;

	flags = 0;
	if (wait_nr || sq_ring_needs_enter(ring, &flags)) {
		if (wait_nr) {
			if (wait_nr > submitted)
				wait_nr = submitted;
			flags |= IORING_ENTER_GETEVENTS;
		}

		ret = io_uring_enter(ring->ring_fd, submitted, wait_nr, flags,
					NULL);
		if (ret < 0)
			return -errno;
	} else
		ret = submitted;

	return ret;
}

static int __io_uring_submit_and_wait(struct io_uring *ring, unsigned wait_nr)
{
	int submitted;

	submitted = __io_uring_flush_sq(ring);
	if (submitted)
		return __io_uring_submit(ring, submitted, wait_nr);

	return 0;
}

/*
 * Submit sqes acquired from io_uring_get_sqe() to the kernel.
 *
 * Returns number of sqes submitted
 */
int io_uring_submit(struct io_uring *ring)
{
	return __io_uring_submit_and_wait(ring, 0);
}

/*
 * Like io_uring_submit(), but allows waiting for events as well.
 *
 * Returns number of sqes submitted
 */
int io_uring_submit_and_wait(struct io_uring *ring, unsigned wait_nr)
{
	return __io_uring_submit_and_wait(ring, wait_nr);
}

/*
 * Return an sqe to fill. Application must later call io_uring_submit()
 * when it's ready to tell the kernel about it. The caller may call this
 * function multiple times before calling io_uring_submit().
 *
 * Returns a vacant sqe, or NULL if we're full.
 */
struct io_uring_sqe *io_uring_get_sqe(struct io_uring *ring)
{
	struct io_uring_sq *sq = &ring->sq;
	unsigned next = sq->sqe_tail + 1;
	struct io_uring_sqe *sqe;

	/*
	 * All sqes are used
	 */
	if (next - sq->sqe_head > *sq->kring_entries)
		return NULL;

	sqe = &sq->sqes[sq->sqe_tail & *sq->kring_mask];
	sq->sqe_tail = next;
	return sqe;
}
