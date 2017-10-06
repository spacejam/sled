#!/usr/bin/gdb --command

"""
a simple python GDB script for running multithreaded
programs in a way that is "deterministic enough"
to tease out and replay interesting bugs.

Tyler Neely 25 Sept 2017
t@jujit.su

references:
    https://sourceware.org/gdb/onlinedocs/gdb/All_002dStop-Mode.html
    https://sourceware.org/gdb/onlinedocs/gdb/Non_002dStop-Mode.html
    https://sourceware.org/gdb/onlinedocs/gdb/Threads-In-Python.html
    https://sourceware.org/gdb/onlinedocs/gdb/Events-In-Python.html
    https://blog.0x972.info/index.php?tag=gdb.py
"""

import gdb
import random

###############################################################################
#                                   config                                    #
###############################################################################
# set this to a number for reproducing results or None to explore randomly
seed = 156112673742  # None  # 951931004895

# set this to the number of valid threads in the program
# {2, 3} assumes a main thread that waits on 2 workers.
# {1, ... N} assumes all of the first N threads are to be explored
threads_whitelist = {2, 3}

# set this to the file of the binary to explore
filename = "target/debug/binary"

# set this to the place the threads should rendezvous before exploring
entrypoint = "src/main.rs:8"

# set this to after the threads are done
exitpoint = "src/main.rs:12"

# invariant unreachable points that should never be accessed
unreachable = [
        "panic_unwind::imp::panic"
        ]

# set this to the locations you want to test interleavings for
interesting = [
        "src/main.rs:8",
        "src/main.rs:9"
        ]

# uncomment this to output the specific commands issued to gdb
gdb.execute("set trace-commands on")

###############################################################################
###############################################################################


class UnreachableBreakpoint(gdb.Breakpoint):
    pass


class DoneBreakpoint(gdb.Breakpoint):
    pass


class InterestingBreakpoint(gdb.Breakpoint):
    pass


class DeterministicExecutor:
    def __init__(self, seed=None):
        if seed:
            print("seeding with", seed)
            self.seed = seed
            random.seed(seed)
        else:
            # pick a random new seed if not provided with one
            self.reseed()

        gdb.execute("file " + filename)

        # non-stop is necessary to provide thread-specific
        # information when breakpoints are hit.
        gdb.execute("set non-stop on")
        gdb.execute("set confirm off")

        self.ready = set()
        self.finished = set()

    def reseed(self):
        random.seed()
        self.seed = random.randrange(1e12)
        print("reseeding with", self.seed)
        random.seed(self.seed)

    def restart(self):
        # reset inner state
        self.ready = set()
        self.finished = set()

        # disconnect callbacks
        gdb.events.stop.disconnect(self.scheduler_callback)
        gdb.events.exited.disconnect(self.exit_callback)

        # nuke all breakpoints
        gdb.execute("d")

        # end execution
        gdb.execute("k")

        # pick new seed
        self.reseed()

        self.run()

    def rendezvous_callback(self, event):
        try:
            self.ready.add(event.inferior_thread.num)
            if len(self.ready) == len(threads_whitelist):
                self.run_schedule()
        except Exception as e:
            # this will be thrown if breakpoint is not a part of event,
            # like when the event was stopped for another reason.
            print(e)

    def run(self):
        gdb.execute("b " + entrypoint)

        gdb.events.stop.connect(self.rendezvous_callback)
        gdb.events.exited.connect(self.exit_callback)

        gdb.execute("r")

    def run_schedule(self):
        print("running schedule")
        gdb.execute("d")
        gdb.events.stop.disconnect(self.rendezvous_callback)
        gdb.events.stop.connect(self.scheduler_callback)

        for bp in interesting:
            InterestingBreakpoint(bp)

        for bp in unreachable:
            UnreachableBreakpoint(bp)

        DoneBreakpoint(exitpoint)

        self.pick()

    def pick(self):
        threads = self.runnable_threads()
        if not threads:
            print("restarting execution after running out of valid threads")
            self.restart()
            return

        thread = random.choice(threads)

        gdb.execute("t " + str(thread.num))
        gdb.execute("c")

    def scheduler_callback(self, event):
        if not isinstance(event, gdb.BreakpointEvent):
            print("WTF sched callback got", event.__dict__)
            return

        if isinstance(event.breakpoint, DoneBreakpoint):
            self.finished.add(event.inferior_thread.num)
        elif isinstance(event.breakpoint, UnreachableBreakpoint):
            print("!" * 80)
            print("unreachable breakpoint triggered with seed", self.seed)
            print("!" * 80)
            gdb.events.exited.disconnect(self.exit_callback)
            gdb.execute("q")
        else:
            print("thread", event.inferior_thread.num,
                  "hit breakpoint at", event.breakpoint.location)

        self.pick()

    def runnable_threads(self):
        threads = gdb.selected_inferior().threads()

        def f(it):
            return (it.is_valid() and not
                    it.is_exited() and
                    it.num in threads_whitelist and
                    it.num not in self.finished)

        good_threads = [it for it in threads if f(it)]
        good_threads.sort(key=lambda it: it.num)

        return good_threads

    def exit_callback(self, event):
        try:
            if event.exit_code != 0:
                print("!" * 80)
                print("interesting exit with seed", self.seed)
                print("!" * 80)
            else:
                print("happy exit")
                self.restart()

            gdb.execute("q")
        except Exception as e:
            pass

de = DeterministicExecutor(seed)
de.run()
