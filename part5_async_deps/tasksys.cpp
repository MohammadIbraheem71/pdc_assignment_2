#include "tasksys.h"
#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <vector>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: Students are not expected to implement TaskSystemParallelSpawn in Part 5.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: Students are not expected to implement TaskSystemParallelSpawn in Part 5.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: Students are not expected to implement TaskSystemParallelSpawn in Part 5.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: Students are not expected to implement TaskSystemParallelSpawn in Part 5.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    // NOTE: Students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part 5.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: Students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part 5.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: Students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part 5.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: Students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part 5.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

struct TaskSystemParallelThreadPoolSleeping::Impl {
    struct Launch {
        IRunnable* runnable;
        int total;                        // tasks in this launch
        int next;                         // next unclaimed task ID
        int done;                         // tasks finished
        int unmet;                        // dependencies not yet finished
        bool finished;
        std::vector<Launch*> dependents;  // launches waiting on this one
        Launch(IRunnable* r, int n)
            : runnable(r), total(n), next(0), done(0), unmet(0), finished(false) {}
    };

    std::vector<std::thread> workers;
    std::mutex mtx;                       // guards everything below
    std::condition_variable cv_work;      // workers sleep here
    std::condition_variable cv_done;      // sync() sleeps here
    std::vector<Launch*> launches;        // index == TaskID
    std::deque<Launch*> ready;            // launches with unclaimed tasks
    int completed;
    bool stop;

    Impl() : completed(0), stop(false) {}

    // Callers of makeReady and completeLaunch hold mtx.
    void makeReady(Launch* l) {
        if (l->total <= 0) {
            completeLaunch(l);            // an empty launch finishes immediately
        } else {
            ready.push_back(l);
        }
    }

    void completeLaunch(Launch* l) {
        l->finished = true;
        completed++;
        for (Launch* d : l->dependents) {
            if (--d->unmet == 0) makeReady(d);
        }
        bool had_dependents = !l->dependents.empty();
        l->dependents.clear();
        if (had_dependents) cv_work.notify_all();
        if (completed == (int)launches.size()) cv_done.notify_all();
    }

    void workerLoop() {
        std::unique_lock<std::mutex> lk(mtx);
        while (true) {
            cv_work.wait(lk, [this] { return stop || !ready.empty(); });
            if (stop) return;

            Launch* l = ready.front();
            int id = l->next++;
            if (l->next == l->total) ready.pop_front();   // no unclaimed tasks left
            IRunnable* r = l->runnable;
            int n = l->total;

            lk.unlock();
            r->runTask(id, n);
            lk.lock();

            l->done++;
            if (l->done == l->total) completeLaunch(l);
        }
    }

    TaskID submit(IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
        std::unique_lock<std::mutex> lk(mtx);
        TaskID id = (TaskID)launches.size();
        Launch* l = new Launch(runnable, num_total_tasks);
        launches.push_back(l);

        for (TaskID d : deps) {
            if (d < 0 || d >= id) continue;   // not a previously submitted launch
            Launch* dep = launches[d];
            if (!dep->finished) {             // checked under the lock completion uses
                l->unmet++;
                dep->dependents.push_back(l);
            }
        }
        if (l->unmet == 0) {
            makeReady(l);
            cv_work.notify_all();
        }
        return id;
    }

    void waitAll() {
        std::unique_lock<std::mutex> lk(mtx);
        cv_done.wait(lk, [this] { return completed == (int)launches.size(); });
    }
};

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    impl_ = new Impl();
    impl_->workers.reserve(num_threads);
    for (int i = 0; i < num_threads; i++) {
        impl_->workers.emplace_back(&Impl::workerLoop, impl_);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    impl_->waitAll();                         // finish outstanding launches first
    {
        std::lock_guard<std::mutex> lk(impl_->mtx);
        impl_->stop = true;
    }
    impl_->cv_work.notify_all();
    for (auto& t : impl_->workers) t.join();
    for (Impl::Launch* l : impl_->launches) delete l;
    delete impl_;
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return impl_->submit(runnable, num_total_tasks, deps);
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    impl_->waitAll();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> no_deps;
    impl_->submit(runnable, num_total_tasks, no_deps);
    impl_->waitAll();
}
