# vanilla-threadpool 🦀

A lightweight, zero-dependency task scheduling thread pool built entirely on top of the Rust standard library (`std`). 

This project demonstrates low-level synchronization, dynamic hardware-concurrency scaling, and defensive concurrency patterns (like bypassing lock poisoning) without using third-party crates like `crossbeam` or `rayon`.

## ✨ Features

- **Zero Dependencies**: Powered exclusively by `std::sync`, `std::thread`, and atomics.
- **Hardware-Aware Scaling**: Automatically detects system capabilities via `thread::available_parallelism` to maximize CPU usage safely.
- **Poison-Resilient Locks**: Bypasses lock poisoning gracefully using `.unwrap_or_else` to ensure healthy worker loops never deadlock.
- **Efficient Resource Management**: Workers block cleanly on a `Condvar` when idle, minimizing CPU consumption until tasks arrive.
- **Safe Graceful Shutdowns**: Signals threads via an atomic flag and cleans up handles properly without hanging, even if tasks remain in the queue.

## 🛠️ Architecture Overview

The system orchestrates work using a thread-safe `SharedState` context wrapped in an `Arc`:

1. **Task Queue**: A `Mutex<VecDeque<Box<dyn FnOnce() + Send>>>` stores generic executable closures.
2. **Worker Synchronization**: A `Condvar` blocks worker threads when work is unavailable, preventing a resource-heavy spinlock.
3. **Atomic Signals**: An `AtomicBool` flags runtime shutdown events securely across multi-thread environments.

---

## 🚀 Quick Start

Ensure you have Rust installed. Clone this repository and run the simulation using cargo:

```bash
cargo run
```

### Running the Test Suite
The project contains a comprehensive unit testing suite covering concurrent task mutations and graceful shutdown safety under heavy loads.

```bash
cargo test
```

---

## 🔬 Core Implementation Spotlight

### Dynamic Scaling & Boundary Fallbacks
The pool dynamically balances thread creation against available hardware concurrency while ensuring it always spawns at least one background worker:

```rust
let logical_threads = match thread::available_parallelism() {
    Ok(n) if n.get() >= 2 => n.get(),
    Ok(_) | Err(_) => 2, // Safely handles single-core setups or platform restrictions
};
```

### Safe Lock Poisoning Handling
If a task panics mid-execution, standard locks become poisoned. This implementation safely extracts the inner data structure to avoid cascades of deadlocked worker threads:

```rust
let mut queue_guard = arc_shared_state_clone
    .queue
    .lock()
    .unwrap_or_else(|poisoned| poisoned.into_inner());
```

---

## 📜 License

This project is open-source and available under the [MIT License](LICENSE).
