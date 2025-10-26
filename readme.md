

## TinyKv 🚀

A tiny, persistent, and concurrent key-value store written in Go.

`TinyKv` is a lightweight, file-based key-value database designed for simplicity and reliability. It provides basic database functionalities with built-in concurrency safety and data recovery mechanisms, making it a great choice for small-scale projects or for learning about database internals.

-----

### Features ✨

  * **File-Based Persistence**: All data is saved directly to files on disk.
  * **Concurrency Safe**: Uses `sync.RWMutex` to safely handle simultaneous read and write operations.
  * **Write-Ahead Log (WAL)**: Implements a `wal.log` for data durability, allowing for state recovery and replay in case of a crash.
  * **Automatic Compaction**: Cleans up old and redundant data by performing compaction when the log file size exceeds a threshold (5MB).
  * **In-Memory Indexing**: Builds an index of key locations in memory on startup for fast lookups.
  * **Last-Write-Wins**: Handles duplicate keys by always referring to the most recent entry.

-----

### How It Works ⚙️

`TinyKv` is built on a log-structured storage model.

1.  **Writing Data**: When you `Set` a key-value pair, the operation is first written to a Write-Ahead Log (`wal.log`) to ensure durability. Then, the data is appended to the current active data file.

2.  **Indexing**: On startup, `TinyKv` reads all the data files and builds an in-memory `map` that stores each key and a pointer to its latest value's location on disk. This index allows for very fast read operations.

3.  **Reading Data**: When you `Get` a key, `TinyKv` looks up its location in the in-memory index and reads the value directly from the corresponding file.

4.  **Durability & Recovery**: The WAL ensures that even if the server crashes before data is fully written to the main files, no data is lost. On the next startup, `TinyKv` replays the WAL to restore its state to what it was before the crash.

5.  **Compaction**: As data is updated or deleted, the data files grow, containing stale information. When the total size exceeds **5MB**, `TinyKv` triggers a compaction process. It creates a new, clean data file containing only the most recent value for each key, and then safely removes the old files, reclaiming disk space.

-----

### Installation

```bash
go get github.com/PRASHANTSWAROOP101/tinykv
```

-----

### Contributing

Contributions are welcome\! Feel free to open an issue or submit a pull request.

-----

### License

This project is licensed under the **MIT License**. See the `LICENSE` file for details.