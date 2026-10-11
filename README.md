# \# CS3006 — Assignment 2: Building a Parallel Task Execution Library

# 

# This archive is the starter code for Assignment 2. It gives you a working

# serial task system, the interfaces you must implement against, a large test

# suite, and reference binaries you can use to sanity-check correctness and

# performance on your own machine.

# 

# The full assignment specification — objectives, the five parts, deliverables,

# and submission instructions — is in the assignment PDF you were given

# separately. This README only covers how to build and run the code.

# 

# \## Folder structure

# 

# ```

# part1to4\_synchronous/   Parts 1-4: Spawn, Spinning pool, Sleeping pool, and the

# &#x20;                        granularity/scheduling investigation.

# &#x20;                        Implement TaskSystemParallelSpawn,

# &#x20;                        TaskSystemParallelThreadPoolSpinning, and

# &#x20;                        TaskSystemParallelThreadPoolSleeping here.

# 

# part5\_async\_deps/       Part 5: asynchronous task graphs with dependencies.

# &#x20;                        Implement runAsyncWithDeps() and sync() (and keep

# &#x20;                        run() correct) on TaskSystemParallelThreadPoolSleeping

# &#x20;                        here.

# 

# common/                  Shared helper code (timers, PPM image writer).

# tests/                   The test suite (tests.h) and test harness.

# tutorial/                A short primer on C++ threads, mutexes, and

# &#x20;                        condition variables. Read this before you start if

# &#x20;                        you haven't used std::thread before.

# ```

# 

# Do not rename files, change public method signatures in `itasksys.h`, or

# restructure the folders above — the test harness depends on this layout.

# 

# \## Building

# 

# Each of `part1to4\_synchronous/` and `part5\_async\_deps/` builds independently:

# 

# ```

# cd part1to4\_synchronous

# make

# ./runtasks -n 8 mandelbrot\_chunked

# ```

# 

# ```

# cd part5\_async\_deps

# make

# ./runtasks -n 8 ping\_pong\_equal\_async

# ```

# 

# `-n` sets the maximum number of worker threads your task system is allowed to

# use. Run `./runtasks -?` to see the full list of available tests.

# 

# \## Checking your work

# 

# A prebuilt \*\*reference implementation\*\* binary is included for Linux (x86 and

# arm), and macOS (Intel and Apple Silicon) in each folder

# (`runtasks\_ref\_linux`, `runtasks\_ref\_linux\_arm`, `runtasks\_ref\_osx\_x86`,

# `runtasks\_ref\_osx\_arm`). You can run it the same way as `./runtasks` to

# compare your output and timing against a correct implementation on the same

# workload, on your own machine. Note that the reference binary does not include

# the `simple\_test\_sync` and `simple\_test\_async` tests; those run only on your

# own `./runtasks`.

# 

# A Python test harness is also provided:

# 

# ```

# cd part1to4\_synchronous      # (or part5\_async\_deps)

# make

# python3 ../tests/run\_test\_harness.py -n 8

# ```

# 

# It runs a batch of tests against both your binary and the reference binary

# and prints a side-by-side comparison. This is a tool for your own debugging.

# 

# \## Adding your own tests

# 

# `tests/tests.h` contains the full test suite plus a `YourTask` /

# `yourTest()` skeleton you can copy to write your own test cases (required for

# Parts 1, 4, and 5 — see the assignment PDF for specifics). Register your new

# tests in `tests/main.cpp`.

