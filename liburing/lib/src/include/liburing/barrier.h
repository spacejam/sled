#ifndef LIBURING_BARRIER_H
#define LIBURING_BARRIER_H

/*
From the kernel documentation file refcount-vs-atomic.rst:

A RELEASE memory ordering guarantees that all prior loads and
stores (all po-earlier instructions) on the same CPU are completed
before the operation. It also guarantees that all po-earlier
stores on the same CPU and all propagated stores from other CPUs
must propagate to all other CPUs before the release operation
(A-cumulative property). This is implemented using
:c:func:`smp_store_release`.

An ACQUIRE memory ordering guarantees that all post loads and
stores (all po-later instructions) on the same CPU are
completed after the acquire operation. It also guarantees that all
po-later stores on the same CPU must propagate to all other CPUs
after the acquire operation executes. This is implemented using
:c:func:`smp_acquire__after_ctrl_dep`.
*/

/* From tools/include/linux/compiler.h */
/* Optimization barrier */
/* The "volatile" is due to gcc bugs */
#define io_uring_barrier()	__asm__ __volatile__("": : :"memory")

/* From tools/virtio/linux/compiler.h */
#define IO_URING_WRITE_ONCE(var, val) \
	(*((volatile __typeof(val) *)(&(var))) = (val))
#define IO_URING_READ_ONCE(var) (*((volatile __typeof(var) *)(&(var))))


#if defined(__x86_64__) || defined(__i386__)
/* Adapted from arch/x86/include/asm/barrier.h */
#define io_uring_mb()		asm volatile("mfence" ::: "memory")
#define io_uring_rmb()		asm volatile("lfence" ::: "memory")
#define io_uring_wmb()		asm volatile("sfence" ::: "memory")
#define io_uring_smp_rmb()	io_uring_barrier()
#define io_uring_smp_wmb()	io_uring_barrier()
#if defined(__i386__)
#define io_uring_smp_mb()	asm volatile("lock; addl $0,0(%%esp)" \
					     ::: "memory", "cc")
#else
#define io_uring_smp_mb()	asm volatile("lock; addl $0,-132(%%rsp)" \
					     ::: "memory", "cc")
#endif

#define io_uring_smp_store_release(p, v)	\
do {						\
	io_uring_barrier();			\
	IO_URING_WRITE_ONCE(*(p), (v));		\
} while (0)

#define io_uring_smp_load_acquire(p)			\
({							\
	__typeof(*p) ___p1 = IO_URING_READ_ONCE(*(p));	\
	io_uring_barrier();				\
	___p1;						\
})

#elif defined(__aarch64__)
/* Adapted from arch/arm64/include/asm/barrier.h */
#define io_uring_dmb(opt)	asm volatile("dmb " #opt : : : "memory")
#define io_uring_dsb(opt)	asm volatile("dsb " #opt : : : "memory")

#define io_uring_mb()		io_uring_dsb(sy)
#define io_uring_rmb()		io_uring_dsb(ld)
#define io_uring_wmb()		io_uring_dsb(st)
#define io_uring_smp_mb()	io_uring_dmb(ish)
#define io_uring_smp_rmb()	io_uring_dmb(ishld)
#define io_uring_smp_wmb()	io_uring_dmb(ishst)

#else /* defined(__x86_64__) || defined(__i386__) || defined(__aarch64__) */
/*
 * Add arch appropriate definitions. Be safe and use full barriers for
 * archs we don't have support for.
 */
#define io_uring_smp_rmb()	__sync_synchronize()
#define io_uring_smp_wmb()	__sync_synchronize()
#endif /* defined(__x86_64__) || defined(__i386__) */

/* From tools/include/asm/barrier.h */

#ifndef io_uring_smp_store_release
#define io_uring_smp_store_release(p, v)	\
do {						\
	io_uring_smp_mb();			\
	IO_URING_WRITE_ONCE(*p, v);		\
} while (0)
#endif

#ifndef io_uring_smp_load_acquire
#define io_uring_smp_load_acquire(p)			\
({							\
	__typeof(*p) ___p1 = IO_URING_READ_ONCE(*p);	\
	io_uring_smp_mb();				\
	___p1;						\
})
#endif

#endif /* defined(LIBURING_BARRIER_H) */
