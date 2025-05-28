#!/usr/bin/env python
"""
ObliviStore Run Script

This script provides a convenient interface to run the ObliviStore system.
It handles different execution modes and command-line arguments.
"""

import argparse
import asyncio
import os
import json
import time
from oblivistore_implementation import ObliviStoreSystem, process_query_file
import math

def compute_partition_capacity(num_blocks, num_partitions):
    per_partition_blocks = math.ceil(num_blocks / num_partitions)

    # Find minimum level count L such that sum(2^i for i in range(L)) >= per_partition_blocks
    L = 1
    while (2 ** L - 1) < per_partition_blocks:
        L += 1

    # Partition capacity is 2^(2L), as required by the ORAM partition level design
    partition_capacity = 2 ** (2 * L)

    return partition_capacity

async def run_interactive(args):
    """Run the system in interactive mode"""
    print("ObliviStore System - Interactive Mode")
    
    # Get system parameters
    num_partitions = int(input("Enter number of partitions: ") or "4")
    num_blocks = int(input("Enter number of blocks to initialize: ") or "16")
    
    # Auto-calculate partition capacity
    partition_capacity = compute_partition_capacity(num_blocks, num_partitions)
    print(f"Initializing system with {args.blocks} blocks...")
    print(f"Auto-calculated partition capacity: {partition_capacity}")
    
    # Create and initialize the system
    system = ObliviStoreSystem(num_partitions, partition_capacity)
    print(f"Initializing system with {num_blocks} blocks...")
    await system.initialize(num_blocks)
    print("System initialized successfully!")
    
    # Interactive loop
    client_id = 0
    
    while True:
        print("\nEnter a command (or 'help' for commands, 'exit' to quit):")
        cmd = input("> ").strip()
        
        if cmd.lower() == 'exit':
            break
            
        elif cmd.lower() == 'help':
            print("Commands:")
            print("  client <id>      - Switch to client with ID")
            print("  R <block_id>     - Read a block")
            print("  W <block_id> <value> - Write a value to a block")
            print("  file <filename>  - Process queries from a file")
            print("  stats            - Show system statistics")
            print("  exit             - Exit the program")
            
        elif cmd.lower().startswith('client '):
            try:
                client_id = int(cmd.split()[1])
                print(f"Switched to client {client_id}")
            except:
                print("Invalid client ID")
                
        elif cmd.lower().startswith('file '):
            try:
                filename = cmd.split()[1]
                if not os.path.exists(filename) and not filename.startswith("queries/"):
                    filename = f"queries/{filename}"
                print(f"Processing queries from file {filename} for client {client_id}")
                results = await process_query_file(system, client_id, filename)
                for i, result in enumerate(results):
                    print(f"Query {i+1} result: {result}")
            except Exception as e:
                print(f"Error processing file: {str(e)}")
                
        elif cmd.lower() == 'stats':
            print("System Statistics:")
            print(f"  Number of partitions: {len(system.oram.partitions)}")
            print(f"  Number of clients: {len(system.clients)}")
            blocks_in_position_map = len(system.oram.position_map.map)
            print(f"  Blocks in position map: {blocks_in_position_map}")
            
        elif cmd:
            # Process as query
            result = await system.process_query(client_id, cmd)
            print(result)
    
    # Shutdown
    print("Shutting down system...")
    await system.shutdown()
    print("System shut down")

async def run_batch(args):
    """Run the system in batch mode"""
    # Load configuration
    if args.config:
        try:
            config_path = args.config
            if not os.path.exists(config_path) and not config_path.startswith("config/"):
                config_path = f"config/{config_path}"
            with open(config_path, 'r') as f:
                config = json.load(f)
                print(f"Loaded configuration from {config_path}")
        except Exception as e:
            print(f"Error loading config file: {str(e)}")
            config = {
                "num_partitions": args.partitions,
                "num_blocks": args.blocks,
                "num_clients": args.clients,
                "query_files": args.query_files or []
            }
    else:
        config = {
            "num_partitions": args.partitions,
            "num_blocks": args.blocks,
            "num_clients": args.clients,
            "query_files": args.query_files or []
        }
    
    # Validate configuration
    if not config.get("query_files"):
        print("No query files specified. Exiting.")
        return
    
    # Create and initialize system
    print("Creating ObliviStore system...")
    num_partitions = config.get("num_partitions", args.partitions)
    num_blocks = config.get("num_blocks", args.blocks)
    
    # Auto-calculate partition capacity
    partition_capacity = compute_partition_capacity(num_blocks, num_partitions)
    print(f"Auto-calculated partition capacity: {partition_capacity}")
    
    system = ObliviStoreSystem(num_partitions, partition_capacity)
    
    print(f"Initializing system with {num_blocks} blocks...")
    await system.initialize(num_blocks)
    print("System initialized successfully!")
    
    # Create clients
    num_clients = config.get("num_clients", args.clients)
    print(f"Creating {num_clients} clients...")
    clients = []
    for i in range(num_clients):
        client = system.add_client(i)
        clients.append(client)
    
    # Process query files
    query_files = config.get("query_files", [])
    print(f"Processing {len(query_files)} query files...")
    
    # Process sequentially or in parallel
    if args.parallel:
        # Process in parallel
        tasks = []
        for i, query_file in enumerate(query_files):
            if i < num_clients:
                print(f"Starting processing of {query_file} for client {i}...")
                task = asyncio.create_task(process_query_file(system, i, query_file))
                tasks.append((i, query_file, task))
        
        # Wait for tasks to complete
        for i, query_file, task in tasks:
            try:
                results = await task
                print(f"Results for client {i} from {query_file}:")
                for j, result in enumerate(results):
                    print(f"  Query {j+1}: {result}")
            except Exception as e:
                print(f"Error processing {query_file} for client {i}: {str(e)}")
    else:
        # Process sequentially
        for i, query_file in enumerate(query_files):
            if i < num_clients:
                client_id = i
            else:
                client_id = i % num_clients
            
            print(f"Processing {query_file} for client {client_id}...")
            try:
                results = await process_query_file(system, client_id, query_file)
                print(f"Results for client {client_id} from {query_file}:")
                for j, result in enumerate(results):
                    print(f"  Query {j+1}: {result}")
            except Exception as e:
                print(f"Error processing {query_file} for client {client_id}: {str(e)}")
    
    # Shutdown
    print("Shutting down system...")
    await system.shutdown()
    print("System shut down")

async def run_test_case(args):
    """Run specific test cases"""
    print(f"Running test case: {args.test_case}")
    
    # Create and initialize system
    num_partitions = args.partitions
    num_blocks = args.blocks
    
    # Auto-calculate partition capacity
    partition_capacity = compute_partition_capacity(num_blocks, num_partitions)
    print(f"Auto-calculated partition capacity: {partition_capacity}")
    
    system = ObliviStoreSystem(num_partitions, partition_capacity)
    await system.initialize(num_blocks)
    print("System initialized")
    
    if args.test_case == "single_read":
        # Test case: Single client read operation
        client = system.add_client(0)
        block_id = 4  # 
        
        print(f"Reading block {block_id}...")
        value, partition_id = await client.read(block_id)
        print(f"Partition: P{partition_id}, Value: {value}, Read of block {block_id} is successful.")
        
        # Read again to verify different path/partition
        print(f"Reading block {block_id} again...")
        value, new_partition_id = await client.read(block_id)
        print(f"Partition: P{new_partition_id}, Value: {value}, Read of block {block_id} is successful.")
        
        if partition_id != new_partition_id:
            print("Success: Different partitions used for same block on consecutive reads.")
        else:
            print("Note: Same partition used for consecutive reads (could happen by chance).")
            
    elif args.test_case == "single_write":
        # Test case: Single client write operation
        client = system.add_client(0)
        block_id = 4  # As per your example
        write_value = "v2"
        
        print(f"Writing value '{write_value}' to block {block_id}...")
        success, partition_id = await client.write(block_id, write_value)
        print(f"Partition: P{partition_id}, Acknowledgement: Write of block {block_id} is successful.")
        
        print(f"Reading block {block_id} to verify write...")
        value, new_partition_id = await client.read(block_id)
        print(f"Partition: P{new_partition_id}, Value: {value}, Read of block {block_id} is successful.")
        
        if value == write_value:
            print("Success: Read value matches written value.")
        else:
            print(f"Error: Read value '{value}' does not match written value '{write_value}'.")
            
        if partition_id != new_partition_id:
            print("Success: Different partitions used for write and subsequent read.")
        else:
            print("Note: Same partition used for write and read (could happen by chance).")
            
    elif args.test_case == "multi_read":
        # Test case: Multiple clients reading blocks
        num_clients = 10
        clients = [system.add_client(i) for i in range(num_clients)]
        
        # Have each client read a different block
        tasks = []
        for i, client in enumerate(clients):
            block_id = i
            task = asyncio.create_task(with_timeout(client.read(block_id), timeout_seconds=5))
            tasks.append(task)
        
        # Wait for all reads to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        print("All multi-read tasks completed.")

        # Print results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Client {i} read operation failed: {result}")
            else:
                value, partition_id = result
                print(f"Client {i} read block {i} from partition P{partition_id}, value: {value}")
            
    elif args.test_case == "multi_readwrite":
        # Test case: Multiple clients reading and writing blocks
        num_clients = 10
        clients = [system.add_client(i) for i in range(num_clients)]
        
        print("Starting write operations...")
        # Have each client write to a block
        write_tasks = []
        for i, client in enumerate(clients):
            block_id = i
            write_value = f"value_from_client_{i}"
            task = asyncio.create_task(with_timeout(client.write(block_id, write_value), timeout_seconds=5))
            write_tasks.append(task)
        
        # Wait for all writes to complete
        write_results = await asyncio.gather(*write_tasks, return_exceptions=True)
        
        # Print write results
        for i, result in enumerate(write_results):
            if isinstance(result, Exception):
                print(f"Client {i} write operation failed: {result}")
            else:
                success, partition_id = result
                print(f"Client {i} wrote to block {i} through partition P{partition_id}, success: {success}")
        
        print("Starting read operations...")
        # Have each client read a different client's block
        read_tasks = []
        for i, client in enumerate(clients):
            block_id = (i + 1) % num_clients  # Read next client's block
            task = asyncio.create_task(with_timeout(client.read(block_id), timeout_seconds=5))
            read_tasks.append(task)
        
        # Wait for all reads to complete
        read_results = await asyncio.gather(*read_tasks, return_exceptions=True)
        
        # Print read results
        for i, result in enumerate(read_results):
            read_block_id = (i + 1) % num_clients
            if isinstance(result, Exception):
                print(f"Client {i} read operation for block {read_block_id} failed: {result}")
            else:
                value, partition_id = result
                print(f"Client {i} read block {read_block_id} from partition P{partition_id}, value: {value}")
    
    else:
        print(f"Unknown test case: {args.test_case}")
    
    # Shutdown
    await system.shutdown()

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="ObliviStore System")
    
    # Common parameters
    parser.add_argument("--partitions", "-p", type=int, default=4, help="Number of partitions")
    parser.add_argument("--blocks", "-b", type=int, default=16, help="Number of blocks to initialize")
    parser.add_argument("--clients", "-n", type=int, default=1, help="Number of clients")
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--interactive", "-i", action="store_true", help="Run in interactive mode")
    mode_group.add_argument("--batch", "-B", action="store_true", help="Run in batch mode")
    mode_group.add_argument("--test", "-t", dest="test_case", 
                           choices=["single_read", "single_write", "multi_read", "multi_readwrite"], 
                           help="Run specific test case")
    
    # Batch mode options
    parser.add_argument("--config", help="Configuration file for batch mode")
    parser.add_argument("--query-files", "-q", nargs="+", help="Query files for batch mode")
    parser.add_argument("--parallel", action="store_true", help="Process query files in parallel")
    
    args = parser.parse_args()
    
    # Determine mode
    if args.test_case:
        await run_test_case(args)
    elif args.batch:
        await run_batch(args)
    else:  # Default to interactive
        await run_interactive(args)

if __name__ == "__main__":
    asyncio.run(main())