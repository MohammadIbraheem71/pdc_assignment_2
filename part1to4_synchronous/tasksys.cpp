#include "tasksys.h"
#include <algorithm>

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
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
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
    //
    // TODO: Student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    //
    num_threads_ = num_threads; // we just assign a value to the private num_threads_ variable
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: Students will modify the implementation of this
    // method in Part 1.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // if we have no tasks then we just return
    if (num_total_tasks <= 0){
        return;
    }

    // only create as many threads as there r tasks, or num_threads
    int workers = std::min(num_threads_, num_total_tasks);

    // shared schedular state, holds the index of the next unclaimed task
    std::atomic<int> next_task(0);

    // lambda function
    auto worker_fn = [&](){
        while(true){
            int id = next_task.fetch_add(1);
            if (id >= num_total_tasks){
                break;
            }
            runnable->runTask(id, num_total_tasks);
        }
    };

    // we create a vector of threads
    std::vector<std::thread> threads;
    threads.reserve(workers);
    for (int i = 0; i < workers; i++){
        threads.emplace_back(worker_fn);
    }

    // wait for all threads to join back
    for (auto& t : threads){
        t.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
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

// modified this function for part 2 of the assignment
TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = num_threads;
    runnable_ = nullptr;
    total_.store(0);
    next_.store(0);
    done_.store(0);
    stop_.store(false);

    workers_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; i++) {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerLoop, this);
    }
}

// modified this function for part 2 of the assignment
TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    // sets stop_ to true and waits for all workers to join
    stop_.store(true);
    for (auto& t : workers_) {
        t.join();
    }
}

// this function has been added new for part 2 of the assignmetn
void TaskSystemParallelThreadPoolSpinning::workerLoop() {
    while (!stop_.load()) {
        // Spin: unlocked check for available work.
        if (next_.load() >= total_.load()){
            continue;
        }

        int id = -1;
        IRunnable* r = nullptr;
        int n = 0;
        {
            // Authoritative check and claim under the lock.
            // the worker claims one task from the current launch, and the claim is protected by a mutex
            std::lock_guard<std::mutex> lk(mtx_);
            if (next_.load() < total_.load()) {
                id = next_.fetch_add(1);
                r = runnable_;
                n = total_.load();
            }
        }

        // worker runs tasks using its local copies
        if (id >= 0) {
            r->runTask(id, n);
            done_.fetch_add(1);
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0) return;

        {
            // publish the new launch. Workers cannot claim while this lock is held
            std::lock_guard<std::mutex> lk(mtx_);
            runnable_ = runnable;
            done_.store(0);
            next_.store(0);
            total_.store(num_total_tasks);
        }

        // the calling thread spins until every task of this launch has finished
        while (done_.load() < num_total_tasks) {}
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
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

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    num_threads_ = num_threads;
    runnable_ = nullptr;
    total_ = 0;
    next_ = 0;
    done_ = 0;
    stop_ = false;

    workers_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; i++) {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        stop_ = true;
    }
    cv_work_.notify_all();
    for (auto& t : workers_) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::workerLoop() {
    std::unique_lock<std::mutex> lk(mtx_);
    while (true) {
        // Sleep until there is an unclaimed task or the system is shutting down.
        cv_work_.wait(lk, [this] { return stop_ || next_ < total_; });
        if (stop_){
            return;
        }

        int id = next_++;
        IRunnable* r = runnable_;
        int n = total_;

        lk.unlock();                // run the task outside the lock
        r->runTask(id, n);
        lk.lock();

        done_++;
        if (done_ == total_) {
            cv_done_.notify_one();  // wake the calling thread
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    if (num_total_tasks <= 0){
        return;
    }

    std::unique_lock<std::mutex> lk(mtx_);
    runnable_ = runnable;
    total_ = num_total_tasks;
    next_ = 0;
    done_ = 0;
    cv_work_.notify_all();          // new work: wake the workers

    // Sleep until every task of this launch has finished.
    cv_done_.wait(lk, [this] { return done_ == total_; });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: Students will implement this method in Part 5 (see part5_async_deps/).
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: Students will modify the implementation of this method in Part 5 (see part5_async_deps/).
    //

    return;
}
