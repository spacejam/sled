#ifndef LIBURING_COMPAT_H
#define LIBURING_COMPAT_H

#if !defined(CONFIG_HAVE_KERNEL_RWF_T)
typedef int __kernel_rwf_t;
#endif

#endif
