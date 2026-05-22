use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

struct SharedState {
    queue: Mutex<VecDeque<Box<dyn FnOnce() + Send>>>,
    condvar: Condvar,
    keep_running: AtomicBool,
}

impl SharedState {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            keep_running: AtomicBool::new(true),
        }
    }
}
fn spawn_thread_pool(
    logical_threads: usize,
    arc_shared_state: &Arc<SharedState>,
) -> Vec<JoinHandle<()>> {
    // Allocating space for workers threads (logical threads - the main thread)
    let mut worker_handle_vector = Vec::<JoinHandle<()>>::with_capacity(logical_threads - 1);

    // Spawning logical threads minus one worker threads. One is subtracted due to:
    // 1. Having a main thread
    // 2. To not exceed hardware concurrency
    for _index in 0..logical_threads - 1 {
        // Cloning the smart pointer containing the shared state to be given to each thread
        let arc_shared_state_clone = Arc::clone(arc_shared_state);

        // Creating the thread and saving the handle
        worker_handle_vector.push(thread::spawn(move || {
            loop {
                // Acquiring the lock on the queue. If a thread panics while holding the lock,
                // we bypass lock poisoning to prevent deadlocking other healthy threads.
                let mut queue_guard = arc_shared_state_clone
                    .queue
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());

                // Using the condvar to wait until there is work available OR the shutdown signal is given
                while queue_guard.is_empty()
                    && arc_shared_state_clone.keep_running.load(Ordering::Relaxed)
                {
                    queue_guard = arc_shared_state_clone
                        .condvar
                        .wait(queue_guard)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }

                // Immediately after waking/acquiring, check if we need to break the loop
                if queue_guard.is_empty()
                    && !arc_shared_state_clone.keep_running.load(Ordering::Relaxed)
                {
                    break;
                }

                // If there is work, it is popped, the lock is released, and it is executed
                if let Some(task) = queue_guard.pop_front() {
                    // The guard is dropped early so other workers can access the queue while this task runs
                    drop(queue_guard);
                    task();
                }
            }
        }));
    }

    worker_handle_vector
}

fn main() {
    // Establishing number of workers. If the CPU is a single core or the information
    // pertaining its cores is unavailable, we default to 2 threads so at least
    // 1 worker thread is created.
    let logical_threads = match thread::available_parallelism() {
        // Multi-core CPU
        Ok(n) if n.get() >= 2 => n.get(),
        // Single core CPU or if no information on cores can be obtained then default to 2 threads
        Ok(_) | Err(_) => 2,
    };

    let arc_shared_state = Arc::new(SharedState::new());
    let worker_handle_vector = spawn_thread_pool(logical_threads, &arc_shared_state);

    // We simulate pushing tasks to the queue
    {
        // Acquiring the lock on the queue. If a thread panics while holding the lock,
        // we bypass lock poisoning to prevent deadlocking other healthy threads.
        let mut queue_guard = arc_shared_state
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Task pushing
        for task_id in 1..=5 {
            queue_guard.push_back(Box::new(move || {
                println!(
                    "Task {} is executing on thread {:?}",
                    task_id,
                    thread::current().id()
                );
            }));
        }
        // Notify workers that tasks are available
        arc_shared_state.condvar.notify_all();
    }

    // Workers are given some time to process tasks before being shut down
    thread::sleep(Duration::from_millis(500));

    // Workers are signaled to stop
    {
        // Acquiring the lock on the queue. If a thread panics while holding the lock,
        // we bypass lock poisoning to prevent deadlocking other healthy threads.
        let _queue_guard = arc_shared_state
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Changing the atomic flag to false
        arc_shared_state
            .keep_running
            .store(false, Ordering::Relaxed);
        // Workers are notified
        arc_shared_state.condvar.notify_all();
    }

    // Workers are joined
    for (i, handle) in worker_handle_vector.into_iter().enumerate() {
        match handle.join() {
            Ok(_) => println!("Worker {} exited gracefully.", i + 1),
            Err(e) => eprintln!("Worker {} panicked: {:?}", i + 1, e),
        }
    }
}

// --- TESTING SUITE ---
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    // Test 1: Verifies that tasks are actually executed and modify shared values
    #[test]
    fn test_tasks_execute_successfully() {
        let state = Arc::new(SharedState::new());
        let handles = spawn_thread_pool(3, &state); // Spawns 2 workers

        let counter = Arc::new(AtomicUsize::new(0));

        // Push 10 increments to the queue
        {
            let mut queue = state.queue.lock().unwrap();
            for _ in 0..10 {
                let counter_clone = Arc::clone(&counter);
                queue.push_back(Box::new(move || {
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                }));
            }
            state.condvar.notify_all();
        }

        // Allow workers processing time
        thread::sleep(Duration::from_millis(100));

        // Trigger graceful shutdown
        state.keep_running.store(false, Ordering::Relaxed);
        state.condvar.notify_all();

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify all 10 operations completed
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    // Test 2: Verifies threads shutdown gracefully even when the queue still contains unprocessed tasks
    #[test]
    fn test_graceful_shutdown_with_remaining_tasks() {
        let state = Arc::new(SharedState::new());
        let handles = spawn_thread_pool(2, &state); // Spawns 1 worker

        // Fill queue but don't notify immediately
        {
            let mut queue = state.queue.lock().unwrap();
            for _ in 0..20 {
                queue.push_back(Box::new(|| {
                    thread::sleep(Duration::from_millis(10));
                }));
            }
        }

        // Immediately signal stop before execution clears the queue
        state.keep_running.store(false, Ordering::Relaxed);
        state.condvar.notify_all();

        // If the pool is implemented correctly, join shouldn't hang indefinitely
        for handle in handles {
            handle.join().unwrap();
        }
    }
}
