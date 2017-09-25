#!/usr/bin/gdb --command
import gdb
import random

# set this to a number for reproducing results
seed = 815851252263 # None
n_threads = 2

filename = "target/debug/workload"
entrypoint = "src/main.rs:8"
interesting = ["b src/main.rs:8", "b src/main.rs:9"]

if seed:
    random.seed(seed)
else:
    random.seed()
    seed = random.randrange(1e12)
    print("seeding with", seed)
    random.seed(seed)

def get_threads():
    threads = gdb.selected_inferior().threads()

    good_threads = [it for it in threads 
            if it.is_valid() and not it.is_exited() and it.num > 1]
    good_threads.sort(key=lambda it: it.num)

    return good_threads


def pick():
    threads = get_threads()

    if not threads:
        print("no good threads, bailing")
        return

    thread = random.choice(threads)

    print("picking thread", thread.num)

    gdb.execute("t " + str(thread.num))


def stop(event):
    print("in stop, our location:", event.breakpoint.location)
    for bp in event.breakpoints:
        print("\t", bp.location)

    pick()

    gdb.execute("c")


def exit_handler (event):
    if event.exit_code == 0:
        print("happy exit")
    else:
        print("!" * 100)
        print("interesting exit with seed", seed)
        print("!" * 100)
    # print(gdb.lookup_symbol("SeqCst", gdb.block_for_pc("src/main.rs:8")))
    gdb.execute("q")
    # deregister()


def register():
    gdb.events.stop.connect(stop)
    gdb.events.exited.connect(exit_handler)


def deregister():
    gdb.events.stop.disconnect(stop)
    gdb.events.exited.disconnect(exit_handler)

ready = []

def detect_entry(event):
    global ready
    try:
        print("got breakpoint with location:", event.breakpoint.location)
        ready += [event.inferior_thread.num]
    except:
        pass

gdb.execute("file " + filename)
gdb.execute("b " + entrypoint)

gdb.events.stop.connect(detect_entry)

gdb.execute("r")
gdb.execute("set scheduler-locking on")

while True:
    # until we have n_threads blocked:
    #   if some are running:
    #       choose it
    #       run it
    #   else:
    #       run 1
    threads = get_threads()
    if not threads:
        print("running thread 1")
        gdb.execute("t 1") 
        gdb.execute("r") 
        continue

    for thread in threads:
        if thread.num in ready:
            continue
        print("running thread", thread.num)
        gdb.execute("t " + str(thread.num))
        gdb.execute("r")

    if len(ready) == n_threads:
        print("we have enough ready threads")
        break
    else:
        print(ready)

gdb.events.stop.disconnect(detect_entry)
gdb.execute("d")

#gdb.execute("rb load")
for bp in interesting:
    gdb.execute(bp)

register()

pick()

gdb.execute("c")

# gdb.execute("q")
