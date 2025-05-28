#!/usr/bin/env python
"""
ObliviStore Benchmark Script

This script runs performance benchmarks for the ObliviStore implementation.
"""

import asyncio
import time
import random
import json
import os
import matplotlib.pyplot as plt
import numpy as np
from oblivistore_implementation import ObliviStoreSystem

async def run_benchmark(num_partitions, partition_capacity, num_blocks, num_operations=100, num_clients=1):
    """
    Run a benchmark with given parameters
    """
    print(f"Running benchmark with {num_partitions} partitions, {partition_capacity} capacity, {num_blocks} blocks, {num_clients} clients")
    
    # Create and initialize system
    system = ObliviStoreSystem(num_partitions, partition_capacity)
    await system.initialize(num_blocks)
    
    # Create clients
    clients = [system.add_client(i) for i in range(num_clients)]
    
    # Prepare random operations
    operations_per_client = num_operations // num_clients
    all_latencies = []
    start_time = time.time()
    
    # Create tasks for each client
    tasks = []
    for client_idx, client in enumerate(clients):
        tasks.append(asyncio.create_task(
            client_benchmark(client, client_idx, operations_per_client, num_blocks, all_latencies)
        ))
    
    # Wait for all tasks to complete
    await asyncio.gather(*tasks)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Calculate throughput and average latency
    throughput = num_operations / total_time
    avg_latency = sum(all_latencies) / len(all_latencies) if all_latencies else 0
    
    print(f"Benchmark completed:")
    print(f"  Total operations: {num_operations}")
    print(f"  Total time: {total_time:.2f} seconds")
    print(f"  Throughput: {throughput:.2f} ops/sec")
    print(f"  Average latency: {avg_latency:.2f} ms")
    
    # Shut down system
    await system.shutdown()
    
    return {
        "num_partitions": num_partitions,
        "partition_capacity": partition_capacity,
        "num_blocks": num_blocks,
        "num_clients": num_clients,
        "num_operations": num_operations,
        "total_time": total_time,
        "throughput": throughput,
        "avg_latency": avg_latency,
        "all_latencies": all_latencies
    }

async def client_benchmark(client, client_id, num_operations, max_block_id, latencies):
    """Run benchmark operations for a single client"""
    for i in range(num_operations):
        # Choose random operation (70% reads, 30% writes)
        operation = "read" if random.random() < 0.7 else "write"
        block_id = random.randint(0, max_block_id - 1)
        
        start_time = time.time()
        
        if operation == "read":
            value, partition_id = await client.read(block_id)
        else:
            # Write operation
            value = f"data_from_client{client_id}_op{i}"
            success, partition_id = await client.write(block_id, value)
        
        end_time = time.time()
        latency = (end_time - start_time) * 1000  # Convert to ms
        latencies.append(latency)

async def run_scaling_benchmark():
    """
    Run benchmarks to test system scaling with different parameters
    """
    # Parameters to test
    partition_counts = [1, 2, 4, 8, 16]
    client_counts = [1, 2, 4, 8, 16]
    block_counts = [16, 32, 64, 128, 256]
    
    results = []
    
    # 1. Benchmark scaling with number of partitions
    print("\n=== Scaling with number of partitions ===")
    for num_partitions in partition_counts:
        result = await run_benchmark(
            num_partitions=num_partitions,
            partition_capacity=16,
            num_blocks=64,
            num_operations=100,
            num_clients=1
        )
        results.append(result)
    
    # 2. Benchmark scaling with number of clients
    print("\n=== Scaling with number of clients ===")
    for num_clients in client_counts:
        result = await run_benchmark(
            num_partitions=8,
            partition_capacity=16,
            num_blocks=64,
            num_operations=100,
            num_clients=num_clients
        )
        results.append(result)
    
    # 3. Benchmark scaling with number of blocks
    print("\n=== Scaling with number of blocks ===")
    for num_blocks in block_counts:
        result = await run_benchmark(
            num_partitions=8,
            partition_capacity=16,
            num_blocks=num_blocks,
            num_operations=100,
            num_clients=1
        )
        results.append(result)
    
    # Save results to file
    # Create results directory if it doesn't exist
    os.makedirs("results", exist_ok=True)
    with open("results/benchmark_results.json", "w") as f:
        json.dump(results, f, indent=2, default=lambda x: x.tolist() if isinstance(x, np.ndarray) else x)
    
    # Generate plots
    plot_benchmark_results(results)

def plot_benchmark_results(results):
    """
    Generate plots from benchmark results
    """
    # Create figure
    fig, axs = plt.subplots(3, 2, figsize=(15, 15))
    
    # Group results by benchmark type
    partition_results = [r for r in results if r["num_clients"] == 1 and r["num_blocks"] == 64]
    client_results = [r for r in results if r["num_partitions"] == 8 and r["num_blocks"] == 64]
    block_results = [r for r in results if r["num_partitions"] == 8 and r["num_clients"] == 1]
    
    # Sort results
    partition_results.sort(key=lambda x: x["num_partitions"])
    client_results.sort(key=lambda x: x["num_clients"])
    block_results.sort(key=lambda x: x["num_blocks"])
    
    # 1. Throughput vs partitions
    x = [r["num_partitions"] for r in partition_results]
    y = [r["throughput"] for r in partition_results]
    axs[0, 0].plot(x, y, 'o-')
    axs[0, 0].set_title('Throughput vs Number of Partitions')
    axs[0, 0].set_xlabel('Number of Partitions')
    axs[0, 0].set_ylabel('Throughput (ops/sec)')
    axs[0, 0].grid(True)
    
    # 2. Latency vs partitions
    y = [r["avg_latency"] for r in partition_results]
    axs[0, 1].plot(x, y, 'o-')
    axs[0, 1].set_title('Latency vs Number of Partitions')
    axs[0, 1].set_xlabel('Number of Partitions')
    axs[0, 1].set_ylabel('Average Latency (ms)')
    axs[0, 1].grid(True)
    
    # 3. Throughput vs clients
    x = [r["num_clients"] for r in client_results]
    y = [r["throughput"] for r in client_results]
    axs[1, 0].plot(x, y, 'o-')
    axs[1, 0].set_title('Throughput vs Number of Clients')
    axs[1, 0].set_xlabel('Number of Clients')
    axs[1, 0].set_ylabel('Throughput (ops/sec)')
    axs[1, 0].grid(True)
    
    # 4. Latency vs clients
    y = [r["avg_latency"] for r in client_results]
    axs[1, 1].plot(x, y, 'o-')
    axs[1, 1].set_title('Latency vs Number of Clients')
    axs[1, 1].set_xlabel('Number of Clients')
    axs[1, 1].set_ylabel('Average Latency (ms)')
    axs[1, 1].grid(True)
    
    # 5. Throughput vs blocks
    x = [r["num_blocks"] for r in block_results]
    y = [r["throughput"] for r in block_results]
    axs[2, 0].plot(x, y, 'o-')
    axs[2, 0].set_title('Throughput vs Number of Blocks')
    axs[2, 0].set_xlabel('Number of Blocks')
    axs[2, 0].set_ylabel('Throughput (ops/sec)')
    axs[2, 0].grid(True)
    
    # 6. Latency vs blocks
    y = [r["avg_latency"] for r in block_results]
    axs[2, 1].plot(x, y, 'o-')
    axs[2, 1].set_title('Latency vs Number of Blocks')
    axs[2, 1].set_xlabel('Number of Blocks')
    axs[2, 1].set_ylabel('Average Latency (ms)')
    axs[2, 1].grid(True)
    
    # Adjust layout and save
    plt.tight_layout()
    # Create results directory if it doesn't exist
    os.makedirs("results", exist_ok=True)
    plt.savefig('results/benchmark_results.png')
    print("Plots saved to results/benchmark_results.png")

async def main():
    """
    Main benchmark function
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="ObliviStore Benchmark")
    parser.add_argument("--scaling", action="store_true", help="Run scaling benchmark")
    parser.add_argument("--partitions", type=int, default=4, help="Number of partitions")
    parser.add_argument("--capacity", type=int, default=8, help="Partition capacity")
    parser.add_argument("--blocks", type=int, default=16, help="Number of blocks")
    parser.add_argument("--operations", type=int, default=100, help="Number of operations")
    parser.add_argument("--clients", type=int, default=1, help="Number of clients")
    
    args = parser.parse_args()
    
    if args.scaling:
        await run_scaling_benchmark()
    else:
        await run_benchmark(
            num_partitions=args.partitions,
            partition_capacity=args.capacity,
            num_blocks=args.blocks,
            num_operations=args.operations,
            num_clients=args.clients
        )

if __name__ == "__main__":
    asyncio.run(main())