# ObliviStore Implementation

This is an implementation of the ObliviStore ORAM protocol, a pathway Oblivious RAM implementation with asynchronous and parallel shuffling operations.

## Overview

ObliviStore is an ORAM protocol designed to hide access patterns to data stored on untrusted servers. This implementation provides:

- Path ORAM with partition-based storage
- Background shuffling and eviction mechanism
- Multi-client support
- Asynchronous operations
- Detailed visualization of position map and eviction cache state

## Files

- `oblivistore_implementation.py`: The core implementation of ObliviStore
- `run_oblivistore.py`: Command-line interface to run ObliviStore in different modes
- `benchmark_oblivistore.py`: Performance benchmarking script
- `demo_oblivistore.py`: Demo script for presentation purposes

## Running the Demo

For a quick demonstration of the system's functionality, run:

```
python demo_oblivistore.py
```

This will showcase:
- Basic read/write operations
- Privacy properties (path randomization)
- Eviction and background shuffling
- Multiple clients using the system concurrently

## Running the System

The system can be run in several modes:

### Interactive Mode

```
python run_oblivistore.py --interactive
```

This allows you to:
- Read and write blocks
- View the position map and eviction cache
- Process query files
- View system statistics

### Batch Mode

```
python run_oblivistore.py --batch --query-files queries/client1.txt queries/client2.txt
```

This processes queries from the specified files for different clients.

### Test Case Mode

```
python run_oblivistore.py --test single_read
```

Available test cases:
- `single_read`: Test a single read operation
- `single_write`: Test a single write operation
- `multi_read`: Test multiple clients reading blocks
- `multi_readwrite`: Test multiple clients reading and writing blocks
- `show_position_map`: Demonstrate changes to the position map

### Additional Options

- `--partitions N`: Set the number of partitions (default: 4)
- `--capacity N`: Set the capacity of each partition (default: 8)
- `--blocks N`: Set the number of blocks to initialize (default: 16)
- `--clients N`: Set the number of clients (default: 1)
- `--no-shuffling`: Disable background shuffling
- `--parallel`: Process query files in parallel (batch mode)

## Benchmarking

To benchmark the system's performance:

```
python benchmark_oblivistore.py
```

For a full scaling benchmark:

```
python benchmark_oblivistore.py --scaling
```

To compare performance with and without shuffling:

```
python benchmark_oblivistore.py --compare
```

## Implementation Details

### Key Components

- **Block**: Represents a data block in the ORAM
- **Partition**: Represents a partition in the ORAM (with multiple levels)
- **PositionMap**: Tracks which partition and level each block resides in
- **StorageCache**: Temporarily stores blocks read from/to be written to the server
- **EvictionCache**: Temporarily stores blocks before eviction
- **ShufflingBuffer**: Used for locally permuting data blocks for shuffling
- **ServerStorage**: Simulates untrusted server storage
- **PartitionReader**: Handles reading blocks from partitions
- **BackgroundShuffler**: Handles scheduling and performing shuffling jobs
- **ORAMMain**: Main ORAM handler - entry point for client operations
- **ObliviStoreClient**: Client interface for ObliviStore
- **ObliviStoreSystem**: Main system class that manages the ORAM and clients

### Semaphores

The implementation uses several semaphores to control the flow of operations:

- **early_cache_in_semaphore**: For controlling early cache-in operations
- **shuffling_buffer_semaphore**: For controlling shuffling buffer capacity
- **eviction_semaphore**: For controlling eviction operations
- **shuffling_io_semaphore**: For controlling I/O operations during shuffling

### Timeouts

All asynchronous operations use timeouts to prevent deadlocks and ensure responsiveness.

## Security Properties

The ObliviStore implementation provides:

1. **Access Pattern Privacy**: Prevents an observer from determining which blocks are being accessed
2. **Path Randomization**: Each block is assigned to a random path for each access
3. **Periodic Reshuffling**: Background shuffling reorganizes blocks to prevent correlation attacks

## Troubleshooting

If you encounter any issues:

1. Try running with `--no-shuffling` to disable background shuffling
2. Check for timeout errors in the logs
3. Reduce the number of concurrent operations
4. Increase the timeout values in the `with_timeout` function calls

## References

This implementation is based on the ObliviStore paper:
- Emil Stefanov and Elaine Shi. "ObliviStore: High Performance Oblivious Cloud Storage"

## License

This project is available under the MIT License.