# sled safety model

This document applies
[STPA](http://psas.scripts.mit.edu/home/get_file.php?name=STPA_handbook.pdf)-style
hazard analysis to the sled embedded database for the purpose of guiding
design and testing efforts to prevent unacceptable losses.

Outline

* [purpose of analysis](#purpose-of-analysis)
  * [losses](#losses)
  * [system boundary](#system-boundary)
  * [hazards](#hazards)
  * [leading indicators](#leading-indicators)
  * [constraints](#constraints)
* [model of control structure](#model-of-control-structure)
* [identify unsafe control actions](#identify-unsafe-control-actions)
* [identify loss scenarios][#identify-loss-scenarios)
* [resources for learning more about STAMP, STPA, and CAST](#resources)

# Purpose of Analysis

## Losses

We wish to prevent the following undesirable situations:

* data loss
* inconsistent (non-linearizable) data access
* process crash
* resource exhaustion

## System Boundary

We draw the line between system and environment where we can reasonably
invest our efforts to prevent losses.

Inside the boundary:

* codebase
  * put safe control actions into place that prevent losses
* documentation
  * show users how to use sled safely
  * recommend hardware, kernels, user code

Outside the boundary:

* Direct changes to hardware, kernels, user code

## Hazards

These hazards can result in the above losses:

* data may be lost if
  * bugs in the logging system
    * `Db::flush` fails to make previous writes durable
  * bugs in the GC system
    * the old location is overwritten before the defragmented location becomes durable
  * bugs in the recovery system
  * hardare failures
* consistency violations may be caused by
  * transaction concurrency control failure to enforce linearizability (strict serializability)
  * non-linearizable lock-free single-key operations
* panic
  * of user threads
  * IO threads
  * flusher & GC thread
  * indexing
  * unwraps/expects
  * failed TryInto/TryFrom + unwrap
* persistent storage exceeding (2 + N concurrent writers) * logical data size
* in-memory cache exceeding the configured cache size
  * caused by incorrect calculation of cache
* use-after-free
* data race
* memory leak
* integer overflow
* buffer overrun
* uninitialized memory access

## Constraints

# Models of Control Structures

for each control action we have, consider:

1. what hazards happen when we fail to apply it / it does not exist?
2. what hazards happen when we do apply it
3. what hazards happen when we apply it too early or too late?
4. what hazards happen if we apply it for too long or not long enough?

durability model

  * recovery
    * LogIter::max_lsn
      * return None if last_lsn_in_batch >= self.max_lsn
    * batch requirement set to last reservation base + inline len - 1
      * reserve bumps
        * bump_atomic_lsn(&self.iobufs.max_reserved_lsn, reservation_lsn + inline_buf_len as Lsn - 1);

lock-free linearizability model

transactional linearizability (strict serializability) model

panic model

memory usage model

storage usage model

