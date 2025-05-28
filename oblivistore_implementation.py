import asyncio
import random
import time
import os
import threading
import uuid
import json
from typing import Dict, List, Tuple, Set, Optional, Any, Union
from collections import defaultdict
import logging
import math

import functools

# Timeout Function for CPU overloading tasks
async def with_timeout(coro, timeout_seconds=2, retries=1):
    """Execute a coroutine with a timeout and optional retries"""
    for retry in range(retries + 1):
        try:
            return await asyncio.wait_for(coro, timeout=timeout_seconds)
        except asyncio.TimeoutError:
            if retry < retries:
                logger.warning(f"Operation timed out after {timeout_seconds} seconds, retrying ({retry+1}/{retries})")
                continue
            else:
                logger.warning(f"Operation timed out after {timeout_seconds} seconds, max retries reached")
                import traceback
                stack_trace = ''.join(traceback.format_stack())
                logger.warning(f"Stack trace at timeout: {stack_trace}")
                return None
        except Exception as e:
            logger.error(f"Error in operation: {e}")
            import traceback
            stack_trace = ''.join(traceback.format_stack())
            logger.error(f"Stack trace: {stack_trace}")
            return None
    
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ObliviStore")

class Semaphore:
    """
    Custom semaphore implementation to track and log operations
    """
    def __init__(self, value=0, name=""):
        self.value = value
        self.name = name
        self.lock = threading.RLock()  # Changed to RLock
        self.min_value = 0  # Track minimum value for debugging
        self.max_value = value  # Track maximum value for debugging
        logger.info(f"Semaphore '{name}' initialized with value {value}")
    
    def increment(self, amount=1):
        with self.lock:
            self.value += amount
            if self.value > self.max_value:
                self.max_value = self.value
            logger.info(f"Incremented semaphore '{self.name}' by {amount}. New value: {self.value}")
    
    def decrement(self, amount=1):
        with self.lock:
            if self.value < amount:
                logger.warning(f"Cannot decrement semaphore '{self.name}' by {amount} as current value is {self.value}")
                return False
            self.value -= amount
            if self.value < self.min_value:
                self.min_value = self.value
            logger.info(f"Decremented semaphore '{self.name}' by {amount}. New value: {self.value}")
            return True

    def get_value(self):
        with self.lock:
            return self.value

class Block:
    """
    Represents a data block in the ORAM
    """
    def __init__(self, block_id: int, value: Any = None, is_dummy: bool = False):
        self.block_id = block_id
        self.value = value if value is not None else f"data_{block_id}"
        self.is_dummy = is_dummy
    
    def __str__(self):
        return f"Block(id={self.block_id}, value={self.value}, dummy={self.is_dummy})"

class Partition:
    """
    Represents a partition in the ORAM
    """
    def __init__(self, partition_id: int, capacity: int):
        self.partition_id = partition_id
        self.capacity = capacity
        self.levels = []  # Each level is a list of blocks
        # Create levels - each level i can store up to 2^i real blocks
        max_level = (capacity.bit_length() - 1) // 2  # log(N)/2 levels
        
        # Initialize empty levels
        for i in range(max_level + 1):
            level_size = 2 ** i
            level = [Block(-1, None, True) for _ in range(level_size * 2)]  # Each level has dummy blocks
            self.levels.append(level)
            
        # Partition state tracking
        self.counter = 0  # Cp in the paper
        self.job_size = 0  # Jp in the paper
        self.being_shuffled = False  # bShuffle in the paper
        self.dummy_counters = [0] * len(self.levels)  # Dummy block counter for each level
        self.read_unread_flags = [{} for _ in range(len(self.levels))]  # To track which blocks remain to be read
        self.marked_for_shuffling = [False] * len(self.levels)
    
    def is_level_filled(self, level_idx):
        """Check if a level is filled based on counter value"""
        return (self.counter & (1 << level_idx)) != 0
    
    def __str__(self):
        filled_levels = [i for i in range(len(self.levels)) if self.is_level_filled(i)]
        # return f"Partition(id={self.partition_id}, counter={self.counter}, job_size={self.job_size}, shuffling={self.being_shuffled}, filled_levels={filled_levels})"

class PositionMap:
    """
    Tracks which partition and level each block resides in
    """
    def __init__(self):
        self.map = {}  # Maps block_id to (partition_id, level_id, position)
    
    def set_position(self, block_id: int, partition_id: int, level_id: int, position: int):
        self.map[block_id] = (partition_id, level_id, position)
        # logger.info(f"Position map updated: Block {block_id} -> Partition {partition_id}, Level {level_id}, Position {position}")
    
    def get_position(self, block_id: int) -> Tuple[int, int, int]:
        if block_id not in self.map:
            return None
        return self.map[block_id]
    
    def remove_position(self, block_id: int):
        if block_id in self.map:
            del self.map[block_id]
            # logger.info(f"Position map: Block {block_id} position removed")

class StorageCache:
    """
    Temporarily stores blocks read from/to be written to the server
    """
    def __init__(self, max_size: int = 1000):
        self.cache = {}  # Maps address to block
        self.max_size = max_size
        self.lock = threading.RLock()  # Changed to RLock
    
    async def cache_in(self, addr: str, server_storage) -> Block:
        """Read block from server and store in cache"""
        with self.lock:
            if len(self.cache) >= self.max_size:
                logger.warning("Storage cache full, cannot cache in more blocks")
                return None
            
            # Simulate reading from server
            block = await server_storage.read(addr)
            if block:
                self.cache[addr] = block
                logger.info(f"Cached in block from address {addr}: {block}")
                return block
            return None
    
    async def cache_out(self, addr: str, server_storage):
        """Write block from cache to server"""
        with self.lock:
            if addr not in self.cache:
                logger.warning(f"No block at address {addr} in storage cache")
                return False
            
            block = self.cache[addr]
            # Re-encrypt block before writing back (simulated)
            success = await server_storage.write(addr, block)
            if success:
                del self.cache[addr]
                logger.info(f"Cached out block to address {addr}: {block}")
                return True
            return False
    
    def fetch(self, addr: str) -> Block:
        """Synchronously fetch a block that already exists in the cache"""
        with self.lock:
            if addr not in self.cache:
                logger.warning(f"No block at address {addr} in storage cache")
                return None
            
            block = self.cache[addr]
            logger.info(f"Fetched block from storage cache at address {addr}: {block}")
            return block
    
    def store(self, addr: str, block: Block):
        """Synchronously store a block to the local cache"""
        with self.lock:
            if len(self.cache) >= self.max_size and addr not in self.cache:
                logger.warning("Storage cache full, cannot store more blocks")
                return False
            
            self.cache[addr] = block
            logger.info(f"Stored block to storage cache at address {addr}: {block}")
            return True


class EvictionCache:
    """
    Temporarily stores blocks before eviction
    """
    def __init__(self, max_size: int = 1000):
        self.cache = {}  # Maps block_id to (block, partition_id) tuples
        self.pending_blocks = set()  # Set of block_ids pending eviction
        self.max_size = max_size
        self.lock = threading.RLock()  # Changed to RLock
        self.ops_in_progress = set()  # Track operations to prevent concurrent access
    
    async def add_block_async(self, block: Block, partition_id: int):
        """Add a block to the eviction cache with async locking"""
        op_id = f"add_{block.block_id}"
        while op_id in self.ops_in_progress:
            await asyncio.sleep(0.01)  # Wait for any in-progress operation on this block
        
        with self.lock:
            self.ops_in_progress.add(op_id)
        
        try:
            return self.add_block(block, partition_id)
        finally:
            with self.lock:
                if op_id in self.ops_in_progress:  # Check to avoid KeyError
                    self.ops_in_progress.remove(op_id)
    
    def add_block(self, block: Block, partition_id: int):
        """Add a block to the eviction cache"""
        with self.lock:
            if len(self.cache) >= self.max_size:
                logger.warning("Eviction cache full, cannot add more blocks")
                return False
            
            self.cache[block.block_id] = (block, partition_id)
            self.pending_blocks.add(block.block_id)  # Mark as pending eviction
            logger.info(f"Added block {block.block_id} with value '{block.value}' to eviction cache, assigned to partition {partition_id}")
            return True
    
    async def get_blocks_for_partition_async(self, partition_id: int, count: int) -> List[Block]:
        """Get up to 'count' blocks assigned to the specified partition with async locking"""
        op_id = f"get_partition_{partition_id}"
        while op_id in self.ops_in_progress:
            await asyncio.sleep(0.01)  # Wait for any in-progress operation on this partition
        
        with self.lock:
            self.ops_in_progress.add(op_id)
        
        try:
            return self.get_blocks_for_partition(partition_id, count)
        finally:
            with self.lock:
                if op_id in self.ops_in_progress:  # Check to avoid KeyError
                    self.ops_in_progress.remove(op_id)
    
    def get_blocks_for_partition(self, partition_id: int, count: int) -> List[Block]:
        """Get up to 'count' blocks assigned to the specified partition"""
        blocks = []
        real_blocks = []
        dummy_blocks = []
        
        with self.lock:
            logger.info(f"Eviction cache: Looking for {count} blocks for partition {partition_id}")
            
            # Find blocks for this partition
            block_ids_to_remove = []
            for block_id, (block, p_id) in list(self.cache.items()):
                if p_id == partition_id and len(real_blocks) < count:
                    real_blocks.append(block)
                    block_ids_to_remove.append(block_id)
                    logger.info(f"Retrieved real block {block_id} with value '{block.value}' from eviction cache for partition {partition_id}")
            
            # Remove blocks from cache
            for block_id in block_ids_to_remove:
                del self.cache[block_id]
                if block_id in self.pending_blocks:
                    self.pending_blocks.remove(block_id)
                    logger.info(f"Removed block {block_id} from pending blocks")
            
            # If not enough real blocks, pad with dummy blocks
            dummy_count = count - len(real_blocks)
            if dummy_count > 0:
                logger.info(f"Adding {dummy_count} dummy blocks to pad eviction for partition {partition_id}")
                for i in range(dummy_count):
                    dummy_block = Block(-1, f"dummy_{partition_id}_{i}", True)
                    dummy_blocks.append(dummy_block)
            
            # Combine real and dummy blocks
            blocks = real_blocks + dummy_blocks
            logger.info(f"Returning {len(real_blocks)} real blocks and {len(dummy_blocks)} dummy blocks for partition {partition_id}")
        
        return blocks
        

    
    def has_block(self, block_id: int) -> bool:
        """Check if a block exists in the eviction cache"""
        with self.lock:
            return block_id in self.cache
    
    def get_block(self, block_id: int) -> Tuple[Block, int]:
        """Get a specific block from the eviction cache if it exists"""
        with self.lock:
            if block_id in self.cache:
                return self.cache[block_id]
            return None, None
    
    def get_cache_status(self):
        """Get a summary of the eviction cache contents"""
        with self.lock:
            block_counts = {}
            for block_id, (_, partition_id) in self.cache.items():
                if partition_id not in block_counts:
                    block_counts[partition_id] = 0
                block_counts[partition_id] += 1
            
            return {
                "total_blocks": len(self.cache),
                "partition_counts": block_counts,
                "pending_blocks": len(self.pending_blocks)
            }

class ShufflingBuffer:
    """
    Used for locally permuting data blocks for shuffling
    """
    def __init__(self, max_size: int = 1000):
        self.buffer = []
        self.max_size = max_size
        self.lock = threading.RLock()  # Changed to RLock
    
    def add_block(self, block: Block) -> bool:
        """Add a block to the shuffling buffer"""
        with self.lock:
            if len(self.buffer) >= self.max_size:
                logger.warning("Shuffling buffer full, cannot add more blocks")
                return False
            
            self.buffer.append(block)
            logger.info(f"Added block to shuffling buffer: {block}")
            return True
    
    def add_multiple_blocks(self, blocks: List[Block]) -> bool:
        """Add multiple blocks to the shuffling buffer"""
        with self.lock:
            if len(self.buffer) + len(blocks) > self.max_size:
                logger.warning("Cannot add blocks, would exceed shuffling buffer capacity")
                return False
            
            self.buffer.extend(blocks)
            logger.info(f"Added {len(blocks)} blocks to shuffling buffer")
            return True
    
    def pad_to_size(self, size: int):
        """Add dummy blocks to pad the buffer to the specified size"""
        with self.lock:
            if size <= len(self.buffer):
                return
            
            count = size - len(self.buffer)
            for _ in range(count):
                dummy = Block(-1, None, True)
                self.buffer.append(dummy)
            
            logger.info(f"Padded shuffling buffer with {count} dummy blocks")
    
    def permute(self):
        """Randomly permute the blocks in the buffer"""
        with self.lock:
            random.shuffle(self.buffer)
            logger.info(f"Permuted {len(self.buffer)} blocks in shuffling buffer")
    
    def get_blocks(self, count: int) -> List[Block]:
        """Get up to 'count' blocks from the buffer"""
        with self.lock:
            if count <= 0:
                return []
            
            if count > len(self.buffer):
                count = len(self.buffer)
            
            blocks = self.buffer[:count]
            self.buffer = self.buffer[count:]
            logger.info(f"Retrieved {len(blocks)} blocks from shuffling buffer")
            return blocks
    
    def clear(self):
        """Clear the buffer"""
        with self.lock:
            count = len(self.buffer)
            self.buffer = []
            logger.info(f"Cleared shuffling buffer ({count} blocks)")

class ServerStorage:
    """
    Simulates untrusted server storage
    """
    def __init__(self):
        self.storage = {}  # Maps address to blocks
        self.lock = threading.Lock()
    
    async def read(self, addr: str) -> Block:
        """Read a block from storage"""
        await asyncio.sleep(0.01)  # Simulate I/O latency
        with self.lock:
            if addr not in self.storage:
                logger.warning(f"No block at address {addr} in server storage")
                return None
            
            block = self.storage[addr]
            logger.info(f"Read block from server storage at address {addr}: {block}")
            return block
    
    async def write(self, addr: str, block: Block) -> bool:
        """Write a block to storage"""
        await asyncio.sleep(0.01)  # Simulate I/O latency
        with self.lock:
            self.storage[addr] = block
            logger.info(f"Wrote block to server storage at address {addr}: {block}")
            return True
    
    def get_all_blocks(self) -> Dict[str, Block]:
        """Get all blocks in storage (for debugging)"""
        with self.lock:
            return dict(self.storage)

class PartitionReader:
    """
    Handles reading blocks from partitions
    """
    def __init__(self, storage_cache, server_storage, position_map, early_cache_in_semaphore):
        self.storage_cache = storage_cache
        self.server_storage = server_storage
        self.position_map = position_map
        self.early_cache_in_semaphore = early_cache_in_semaphore
    
    async def read_partition(self, partition: Partition, block_id: int) -> Block:
        """
        Read a block from a partition (implements Figure 6 algorithm)
        """
        logger.info(f"Reading block {block_id} from partition {partition.partition_id}")
        
        # 1) Look up the position map to determine the level where blockid resides
        position_info = self.position_map.get_position(block_id)
        if position_info and position_info[0] == partition.partition_id:
            target_level_id, target_position = position_info[1], position_info[2]
            logger.info(f"Block {block_id} found in position map: level {target_level_id}, position {target_position}")
        else:
            # Block is not in this partition or is a dummy request
            target_level_id, target_position = None, None
            logger.info(f"Block {block_id} not found in position map for partition {partition.partition_id}")
        
        # 2) For each level in partition that satisfies conditions: increment early cache-in semaphore
        level_increments = 0
        for level_id, level in enumerate(partition.levels):
            if (not partition.is_level_filled(level_id) or  # Level is empty
                not partition.marked_for_shuffling[level_id] or  # Level not marked for shuffling 
                (partition.marked_for_shuffling[level_id] and  # Level marked but all blocks cached
                 all(partition.read_unread_flags[level_id].values()))):
                level_increments += 1
        
        # Decrement early cache-in semaphore in advance for all applicable levels
        self.early_cache_in_semaphore.decrement(level_increments)
        logger.info(f"Decremented early cache-in semaphore by {level_increments}")
        
        # 3) For each filled level in the partition
        result_block = None
        for level_id, level in enumerate(partition.levels):
            if not partition.is_level_filled(level_id):
                continue
                
            if level_id == target_level_id:
                result_block = await with_timeout(
                    self.read_real(partition, level_id, block_id, target_position),
                    timeout_seconds=2,
                    retries=1
                )
            else:
                await with_timeout(
                    self.read_fake(partition, level_id),
                    timeout_seconds=2,
                    retries=1
                )
        
        return result_block
    
    async def read_real(self, partition: Partition, level_id: int, block_id: int, position: int) -> Block:
        """
        Read a real block from a level in a partition
        """
        logger.info(f"Reading real block {block_id} from partition {partition.partition_id}, level {level_id}, position {position}")
        
        # Construct address
        addr = f"p{partition.partition_id}_l{level_id}_pos{position}"
        
        # Check if block has been cached in already
        cached_block = self.storage_cache.fetch(addr)
        if cached_block:
            logger.info(f"Block {block_id} already in storage cache")
            
            # Validate block ID
            if cached_block.block_id != block_id:
                logger.warning(f"Block ID mismatch in storage cache at {addr}: expected {block_id}, got {cached_block.block_id}")
                # Still need to make a fake read to prevent timing attacks
                await self.read_fake(partition, level_id)
                return None
                
            # Still need to make a fake read to prevent timing attacks
            await self.read_fake(partition, level_id)
            return cached_block
        
        # Block not in cache, need to read from server
        logger.info(f"Caching in block {block_id} from server")
        block = await self.storage_cache.cache_in(addr, self.server_storage)
        
        # Validate block ID after reading from server
        if block and block.block_id != block_id:
            logger.warning(f"Block ID mismatch at {addr}: expected {block_id}, got {block.block_id}")
            # Read was already performed, no need for additional fake read
            return None
        
        return block
    
    async def read_fake(self, partition: Partition, level_id: int):
        """
        Read a fake (dummy or early cache-in) block from a level
        """
        logger.info(f"Reading fake block from partition {partition.partition_id}, level {level_id}")
        
        if not partition.marked_for_shuffling[level_id]:
            # Level not being shuffled, read a dummy block
            position = partition.dummy_counters[level_id]
            partition.dummy_counters[level_id] = (position + 1) % (2 * (2 ** level_id))  # Wrap around
            
            addr = f"p{partition.partition_id}_l{level_id}_dummy{position}"
            logger.info(f"Reading dummy block from {addr}")
            await self.storage_cache.cache_in(addr, self.server_storage)
        
        elif any(not flag for flag in partition.read_unread_flags[level_id].values()):
            # Level is being shuffled and has unread blocks, perform early cache-in
            # Find an unread block
            for pos, is_read in partition.read_unread_flags[level_id].items():
                if not is_read:
                    addr = f"p{partition.partition_id}_l{level_id}_pos{pos}"
                    logger.info(f"Performing early cache-in for {addr}")
                    await self.storage_cache.cache_in(addr, self.server_storage)
                    partition.read_unread_flags[level_id][pos] = True
                    break
        
        else:
            # Level being shuffled but all blocks cached, or no unread blocks
            logger.info(f"No fake read needed for level {level_id} (all blocks already cached or no unread blocks)")
            return None

class BackgroundShuffler:
    """
    Handles scheduling and performing shuffling jobs
    """
    def __init__(self, partitions, storage_cache, server_storage, position_map, eviction_cache,
                 shuffling_buffer, early_cache_in_semaphore, shuffling_buffer_semaphore,
                 eviction_semaphore, shuffling_io_semaphore):
        self.partitions = partitions
        self.storage_cache = storage_cache
        self.server_storage = server_storage
        self.position_map = position_map
        self.eviction_cache = eviction_cache
        self.shuffling_buffer = shuffling_buffer
        
        # Semaphores
        self.early_cache_in_semaphore = early_cache_in_semaphore
        self.shuffling_buffer_semaphore = shuffling_buffer_semaphore
        self.eviction_semaphore = eviction_semaphore
        self.shuffling_io_semaphore = shuffling_io_semaphore
        
        self.running = False
        self.shuffling_lock = threading.RLock()  # Added lock for shuffling operations
        self.partitions_being_shuffled = set()  # Track partitions currently being shuffled
    
    async def start(self):
        """Start the background shuffler loop"""
        self.running = True
        while self.running:
            try:
                # Use a shorter timeout to recover faster from issues
                await asyncio.wait_for(self._shuffler_loop(), timeout=1.0)
            except asyncio.TimeoutError:
                logger.warning("Background shuffler loop timed out, attempting to reset state")
                # Try to reset any partitions being shuffled
                with self.shuffling_lock:
                    for p_id in list(self.partitions_being_shuffled):
                        for partition in self.partitions:
                            if partition.partition_id == p_id and partition.being_shuffled:
                                self._reset_shuffling_state(partition)
                # Make sure to increment semaphores to prevent deadlock
                self.shuffling_buffer_semaphore.increment(1)
                self.shuffling_io_semaphore.increment(1)
            except Exception as e:
                logger.warning(f"Background shuffler loop encountered an error: {e}")
                import traceback
                logger.warning(f"Stack trace: {traceback.format_exc()}")
                # Try to reset any partitions being shuffled
                with self.shuffling_lock:
                    for p_id in list(self.partitions_being_shuffled):
                        for partition in self.partitions:
                            if partition.partition_id == p_id and partition.being_shuffled:
                                self._reset_shuffling_state(partition)
                # Make sure to increment semaphores to prevent deadlock
                self.shuffling_buffer_semaphore.increment(1)
                self.shuffling_io_semaphore.increment(1)
            await asyncio.sleep(0.1)
    
    def stop(self):
        """Stop the background shuffler"""
        self.running = False
    
    async def _shuffler_loop(self):
        """
        Implements the background shuffler loop (Figure 7 algorithm)
        """
        try:
            # Find a partition to shuffle using the lock to prevent races
            partition_to_shuffle = None
            with self.shuffling_lock:
                for partition in self.partitions:
                    if (partition.job_size > 0 and 
                        not partition.being_shuffled and 
                        partition.partition_id not in self.partitions_being_shuffled):
                        partition_to_shuffle = partition
                        partition.being_shuffled = True
                        self.partitions_being_shuffled.add(partition.partition_id)
                        break
            
            if not partition_to_shuffle:
                # No partition needs shuffling
                return
            
            logger.info(f"Starting shuffling for partition {partition_to_shuffle.partition_id} with job size {partition_to_shuffle.job_size}")
            
            # Mark levels for shuffling based on current counter and job size
            current_counter = partition_to_shuffle.counter
            new_counter = (current_counter + partition_to_shuffle.job_size) % (2 ** len(partition_to_shuffle.levels))
            
            # Determine which levels need shuffling
            levels_to_shuffle = []
            for i in range(len(partition_to_shuffle.levels)):
                # A level needs shuffling if it changes from filled to empty or vice versa
                if ((current_counter >> i) & 1) != ((new_counter >> i) & 1):
                    partition_to_shuffle.marked_for_shuffling[i] = True
                    levels_to_shuffle.append(i)
                    logger.info(f"Marked level {i} for shuffling in partition {partition_to_shuffle.partition_id}")
            
            # Take snapshot of job size
            job_size_snapshot = partition_to_shuffle.job_size
            partition_to_shuffle.job_size = 0  # Reset job size to accumulate new requests
            logger.info(f"Shuffling partition {partition_to_shuffle.partition_id}: job size snapshot = {job_size_snapshot}")
            
            # 2) Cache-in and reserve space
            # For each unread block in levels marked for shuffling, cache them in
            blocks_to_cache = 0
            for level_id in levels_to_shuffle:
                if partition_to_shuffle.is_level_filled(level_id):
                    level = partition_to_shuffle.levels[level_id]
                    # Initialize read/unread flags for blocks in this level
                    partition_to_shuffle.read_unread_flags[level_id] = {i: False for i in range(len(level))}
                    blocks_to_cache += len(level)
            
            logger.info(f"Shuffling will cache in {blocks_to_cache} blocks from marked levels")
            
            # Reserve space in shuffling buffer
            w = blocks_to_cache + job_size_snapshot  # Total blocks that will be processed
            r = 0  # Number of slots reserved so far
            
            logger.info(f"Need to reserve space for {w} blocks in total (including {job_size_snapshot} from eviction cache)")
            
            # Track successful cache-ins for reliable shuffling
            cached_blocks = []
            
            # Cache in blocks from marked levels
            for level_id in levels_to_shuffle:
                if partition_to_shuffle.is_level_filled(level_id):
                    level = partition_to_shuffle.levels[level_id]
                    logger.info(f"Caching in blocks from level {level_id} (size: {len(level)})")
                    for position in range(len(level)):
                        # Check if we have enough semaphore resources
                        if not self.shuffling_buffer_semaphore.decrement(1) or not self.shuffling_io_semaphore.decrement(1):
                            logger.error("Not enough semaphore resources to continue shuffling")
                            self._reset_shuffling_state(partition_to_shuffle)
                            return
                        
                        # Issue a cache-in request
                        addr = f"p{partition_to_shuffle.partition_id}_l{level_id}_pos{position}"
                        logger.info(f"Caching in block from {addr} for shuffling")
                        
                        try:
                            # Use longer timeout for cache-in, with retries
                            cache_result = await with_timeout(
                                self.storage_cache.cache_in(addr, self.server_storage), 
                                timeout_seconds=2,
                                retries=1
                            )
                            if cache_result is not None:
                                cached_blocks.append((addr, cache_result))
                                r += 1
                                logger.info(f"Successfully cached in block {cache_result.block_id} from {addr}")
                            else:
                                # Cache-in failed, increment the semaphores we decremented
                                self.shuffling_buffer_semaphore.increment(1)
                                self.shuffling_io_semaphore.increment(1)
                                logger.warning(f"Failed to cache in block from {addr}")
                        except Exception as e:
                            logger.error(f"Error during cache-in for {addr}: {e}")
                            self.shuffling_buffer_semaphore.increment(1)
                            self.shuffling_io_semaphore.increment(1)
            
            # Decrement shuffling buffer semaphore for additional space
            if w > r:
                logger.info(f"Reserving additional {w - r} slots in shuffling buffer")
                if not self.shuffling_buffer_semaphore.decrement(w - r):
                    logger.error("Not enough shuffling buffer space")
                    self._reset_shuffling_state(partition_to_shuffle)
                    return
                logger.info(f"Reserved additional {w - r} slots in shuffling buffer")
            
            # 3) Perform atomic shuffle
            logger.info(f"Starting atomic shuffle for partition {partition_to_shuffle.partition_id}")
            
            # Fetch cached-in blocks
            all_cached_blocks = []
            for addr, block in cached_blocks:
                if block:
                    all_cached_blocks.append(block)
                    
                    # For early cache-ins, increment the semaphore
                    position_info = self.position_map.get_position(block.block_id)
                    if position_info is None or position_info[0] != partition_to_shuffle.partition_id:
                        # This was an early cache-in
                        self.early_cache_in_semaphore.increment(1)
                        logger.info(f"Incremented early cache-in semaphore for block {block.block_id}")
            
            logger.info(f"Fetched {len(all_cached_blocks)} cached-in blocks for shuffling")
            
            # Fetch blocks from eviction cache - use async version
            logger.info(f"Fetching up to {job_size_snapshot} blocks from eviction cache for partition {partition_to_shuffle.partition_id}")
            try:
                eviction_blocks = await with_timeout(
                    self.eviction_cache.get_blocks_for_partition_async(
                        partition_to_shuffle.partition_id, job_size_snapshot),
                    timeout_seconds=2,
                    retries=1
                )
                
                # Log details about each block from eviction cache
                real_blocks = [b for b in eviction_blocks if not b.is_dummy]
                dummy_blocks = [b for b in eviction_blocks if b.is_dummy]
                
                logger.info(f"Retrieved {len(real_blocks)} real blocks from eviction cache for partition {partition_to_shuffle.partition_id}")
                for block in real_blocks:
                    logger.info(f"  Real block {block.block_id} with value '{block.value}'")
                
                logger.info(f"Added {len(dummy_blocks)} dummy blocks to pad eviction")
            except Exception as e:
                logger.warning(f"Error retrieving blocks from eviction cache: {e}")
                logger.warning("Falling back to sync version")
                eviction_blocks = self.eviction_cache.get_blocks_for_partition(
                    partition_to_shuffle.partition_id, job_size_snapshot)
            
            real_eviction_blocks = [b for b in eviction_blocks if not b.is_dummy]
            dummy_eviction_blocks = [b for b in eviction_blocks if b.is_dummy]
            logger.info(f"Got {len(real_eviction_blocks)} real blocks and {len(dummy_eviction_blocks)} dummy blocks from eviction cache")
            
            # Increment eviction semaphore accordingly
            self.eviction_semaphore.increment(job_size_snapshot)
            logger.info(f"Incremented eviction semaphore by {job_size_snapshot}")
            
            # Add blocks to shuffling buffer
            self.shuffling_buffer.add_multiple_blocks(all_cached_blocks)
            logger.info(f"Added {len(all_cached_blocks)} cached-in blocks to shuffling buffer")
            self.shuffling_buffer.add_multiple_blocks(eviction_blocks)
            logger.info(f"Added {len(eviction_blocks)} blocks from eviction cache to shuffling buffer")
            
            # Pad buffer to size w
            padding_blocks = w - len(all_cached_blocks) - len(eviction_blocks)
            if padding_blocks > 0:
                logger.info(f"Padding shuffling buffer with {padding_blocks} dummy blocks")
                self.shuffling_buffer.pad_to_size(w)
            
            # Permute the shuffling buffer
            self.shuffling_buffer.permute()
            logger.info(f"Shuffled {w} blocks for partition {partition_to_shuffle.partition_id}")
            
            # Use a set to track used positions in each level to avoid conflicts
            used_positions = {level_id: set() for level_id in range(len(partition_to_shuffle.levels))}
            
            # Store shuffled blocks into storage cache
            for level_id in range(len(partition_to_shuffle.levels)):
                if (new_counter >> level_id) & 1:  # This level should be filled
                    level_size = 2 ** level_id
                    blocks_for_level = self.shuffling_buffer.get_blocks(2 * level_size)
                    logger.info(f"Storing {len(blocks_for_level)} blocks to level {level_id} of partition {partition_to_shuffle.partition_id}")
                    
                    real_blocks = 0
                    dummy_blocks = 0
                    
                    # Store blocks and update position map
                    for i, block in enumerate(blocks_for_level):
                        # Skip if position is already used (find another position)
                        if i in used_positions[level_id]:
                            for alt_pos in range(2 * level_size):
                                if alt_pos not in used_positions[level_id]:
                                    i = alt_pos
                                    break
                        
                        used_positions[level_id].add(i)
                        
                        if not block.is_dummy:
                            real_blocks += 1
                            addr = f"p{partition_to_shuffle.partition_id}_l{level_id}_pos{i}"
                            self.storage_cache.store(addr, block)
                            
                            # Update position map - remove old position and set new one
                            old_position = self.position_map.get_position(block.block_id)
                            if old_position:
                                logger.info(f"Updating position for block {block.block_id} from {old_position} to " + 
                                        f"({partition_to_shuffle.partition_id}, {level_id}, {i})")
                            else:
                                logger.info(f"Setting new position for block {block.block_id}: " + 
                                         f"({partition_to_shuffle.partition_id}, {level_id}, {i})")
                            
                            # Set the new position
                            self.position_map.set_position(block.block_id, partition_to_shuffle.partition_id, level_id, i)
                            
                            # Remove from pending blocks if it was in eviction cache
                            if hasattr(self.eviction_cache, 'pending_blocks') and block.block_id in self.eviction_cache.pending_blocks:
                                with self.eviction_cache.lock:  # Use lock to avoid race conditions
                                    if block.block_id in self.eviction_cache.pending_blocks:
                                        self.eviction_cache.pending_blocks.remove(block.block_id)
                                        logger.info(f"Removed block {block.block_id} from pending blocks in eviction cache")
                            
                            logger.info(f"Stored block {block.block_id} at {addr}")
                        else:
                            dummy_blocks += 1
                    
                    logger.info(f"Level {level_id} filled with {real_blocks} real blocks and {dummy_blocks} dummy blocks")
            
            # Update partition counter and clear shuffling state
            logger.info(f"Updating partition counter from {current_counter} to {new_counter}")
            partition_to_shuffle.counter = new_counter
            for i in range(len(partition_to_shuffle.levels)):
                partition_to_shuffle.marked_for_shuffling[i] = False
            
            # Clean up partition tracking
            with self.shuffling_lock:
                partition_to_shuffle.being_shuffled = False
                if partition_to_shuffle.partition_id in self.partitions_being_shuffled:
                    self.partitions_being_shuffled.remove(partition_to_shuffle.partition_id)
            
            logger.info(f"Completed shuffle for partition {partition_to_shuffle.partition_id}, new counter: {new_counter}")
            
            # 4) Cache-out in the background
            logger.info(f"Starting cache-out operations for partition {partition_to_shuffle.partition_id}")
            cache_out_tasks = []
            for level_id in range(len(partition_to_shuffle.levels)):
                if (new_counter >> level_id) & 1:  # This level is filled
                    level_size = 2 ** level_id
                    for i in range(2 * level_size):
                        # Decrement shuffling I/O semaphore
                        if not self.shuffling_io_semaphore.decrement(1):
                            logger.warning(f"Not enough I/O semaphore resources for cache-out at level {level_id}, pos {i}")
                            self.shuffling_io_semaphore.increment(1)  # Undo decrement
                            continue
                        
                        # Issue cache-out with timeout protection
                        addr = f"p{partition_to_shuffle.partition_id}_l{level_id}_pos{i}"
                        task = asyncio.create_task(self._safe_cache_out(addr))
                        cache_out_tasks.append(task)
            
            logger.info(f"Scheduled {len(cache_out_tasks)} cache-out operations")
            
            # Don't wait for cache-out operations - let them complete in the background
            if cache_out_tasks:
                asyncio.create_task(self._wait_for_cache_out_tasks(cache_out_tasks))
        
        except Exception as e:
            logger.error(f"Error in background shuffler: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            
            # Reset state for the partition being shuffled
            if 'partition_to_shuffle' in locals() and partition_to_shuffle:
                self._reset_shuffling_state(partition_to_shuffle)
                
            # Make sure semaphores are properly restored to prevent deadlock
            try:
                self.shuffling_buffer_semaphore.increment(1)
                self.shuffling_io_semaphore.increment(1)
                logger.info("Incremented semaphores after error to prevent deadlock")
            except Exception as release_error:
                logger.error(f"Error releasing semaphores: {release_error}")

    async def _wait_for_cache_out_tasks(self, tasks):
        """Wait for cache-out tasks to complete in the background"""
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error waiting for cache-out tasks: {e}")

    async def _safe_cache_out(self, addr):
        """Safe wrapper for cache-out operations with proper semaphore handling"""
        try:
            success = await with_timeout(
                self.storage_cache.cache_out(addr, self.server_storage), 
                timeout_seconds=2,
                retries=1
            )
            # Always increment the shuffling buffer semaphore, regardless of success
            self.shuffling_buffer_semaphore.increment(1)
            logger.info(f"Cached out block at {addr}, incremented shuffling buffer semaphore")
        except Exception as e:
            logger.error(f"Error during cache-out for {addr}: {e}")
            # Make sure we increment the semaphore even on error
            self.shuffling_buffer_semaphore.increment(1)
            logger.info(f"Incremented shuffling buffer semaphore after cache-out error")

    def _reset_shuffling_state(self, partition):
        """Reset the shuffling state for a partition after an error"""
        with self.shuffling_lock:
            partition.being_shuffled = False
            for i in range(len(partition.levels)):
                partition.marked_for_shuffling[i] = False
            if partition.partition_id in self.partitions_being_shuffled:
                self.partitions_being_shuffled.remove(partition.partition_id)
        logger.info(f"Reset shuffling state for partition {partition.partition_id}")

class BackgroundShuffler:
    """
    Handles scheduling and performing shuffling jobs
    """
    def __init__(self, partitions, storage_cache, server_storage, position_map, eviction_cache,
                 shuffling_buffer, early_cache_in_semaphore, shuffling_buffer_semaphore,
                 eviction_semaphore, shuffling_io_semaphore):
        self.partitions = partitions
        self.storage_cache = storage_cache
        self.server_storage = server_storage
        self.position_map = position_map
        self.eviction_cache = eviction_cache
        self.shuffling_buffer = shuffling_buffer
        
        # Semaphores
        self.early_cache_in_semaphore = early_cache_in_semaphore
        self.shuffling_buffer_semaphore = shuffling_buffer_semaphore
        self.eviction_semaphore = eviction_semaphore
        self.shuffling_io_semaphore = shuffling_io_semaphore
        
        self.running = False
        self.shuffling_lock = threading.RLock()  # Added lock for shuffling operations
        self.partitions_being_shuffled = set()  # Track partitions currently being shuffled
    
    async def start(self):
        """Start the background shuffler loop"""
        self.running = True
        while self.running:
            try:
                # Use a shorter timeout to recover faster from issues
                await asyncio.wait_for(self._shuffler_loop(), timeout=1.0)
            except asyncio.TimeoutError:
                logger.warning("Background shuffler loop timed out, attempting to reset state")
                # Try to reset any partitions being shuffled
                with self.shuffling_lock:
                    for p_id in list(self.partitions_being_shuffled):
                        for partition in self.partitions:
                            if partition.partition_id == p_id and partition.being_shuffled:
                                self._reset_shuffling_state(partition)
                # Make sure to increment semaphores to prevent deadlock
                self.shuffling_buffer_semaphore.increment(1)
                self.shuffling_io_semaphore.increment(1)
            except Exception as e:
                logger.warning(f"Background shuffler loop encountered an error: {e}")
                import traceback
                logger.warning(f"Stack trace: {traceback.format_exc()}")
                # Try to reset any partitions being shuffled
                with self.shuffling_lock:
                    for p_id in list(self.partitions_being_shuffled):
                        for partition in self.partitions:
                            if partition.partition_id == p_id and partition.being_shuffled:
                                self._reset_shuffling_state(partition)
                # Make sure to increment semaphores to prevent deadlock
                self.shuffling_buffer_semaphore.increment(1)
                self.shuffling_io_semaphore.increment(1)
            await asyncio.sleep(0.1)
    
    def stop(self):
        """Stop the background shuffler"""
        self.running = False
    
    async def _shuffler_loop(self):
        """
        Implements the background shuffler loop (Figure 7 algorithm)
        """
        try:
            # Find a partition to shuffle using the lock to prevent races
            partition_to_shuffle = None
            with self.shuffling_lock:
                for partition in self.partitions:
                    if (partition.job_size > 0 and 
                        not partition.being_shuffled and 
                        partition.partition_id not in self.partitions_being_shuffled):
                        partition_to_shuffle = partition
                        partition.being_shuffled = True
                        self.partitions_being_shuffled.add(partition.partition_id)
                        break
            
            if not partition_to_shuffle:
                # No partition needs shuffling
                return
            
            logger.info(f"Starting shuffling for partition {partition_to_shuffle.partition_id} with job size {partition_to_shuffle.job_size}")
            
            # Mark levels for shuffling based on current counter and job size
            current_counter = partition_to_shuffle.counter
            new_counter = (current_counter + partition_to_shuffle.job_size) % (2 ** len(partition_to_shuffle.levels))
            
            # Determine which levels need shuffling
            levels_to_shuffle = []
            for i in range(len(partition_to_shuffle.levels)):
                # A level needs shuffling if it changes from filled to empty or vice versa
                if ((current_counter >> i) & 1) != ((new_counter >> i) & 1):
                    partition_to_shuffle.marked_for_shuffling[i] = True
                    levels_to_shuffle.append(i)
                    logger.info(f"Marked level {i} for shuffling in partition {partition_to_shuffle.partition_id}")
            
            # Take snapshot of job size
            job_size_snapshot = partition_to_shuffle.job_size
            partition_to_shuffle.job_size = 0  # Reset job size to accumulate new requests
            logger.info(f"Shuffling partition {partition_to_shuffle.partition_id}: job size snapshot = {job_size_snapshot}")
            
            # 2) Cache-in and reserve space
            # For each unread block in levels marked for shuffling, cache them in
            blocks_to_cache = 0
            for level_id in levels_to_shuffle:
                if partition_to_shuffle.is_level_filled(level_id):
                    level = partition_to_shuffle.levels[level_id]
                    # Initialize read/unread flags for blocks in this level
                    partition_to_shuffle.read_unread_flags[level_id] = {i: False for i in range(len(level))}
                    blocks_to_cache += len(level)
            
            logger.info(f"Shuffling will cache in {blocks_to_cache} blocks from marked levels")
            
            # Reserve space in shuffling buffer
            w = blocks_to_cache + job_size_snapshot  # Total blocks that will be processed
            r = 0  # Number of slots reserved so far
            
            logger.info(f"Need to reserve space for {w} blocks in total (including {job_size_snapshot} from eviction cache)")
            
            # Track successful cache-ins for reliable shuffling
            cached_blocks = []
            
            # Cache in blocks from marked levels
            for level_id in levels_to_shuffle:
                if partition_to_shuffle.is_level_filled(level_id):
                    level = partition_to_shuffle.levels[level_id]
                    logger.info(f"Caching in blocks from level {level_id} (size: {len(level)})")
                    for position in range(len(level)):
                        # Check if we have enough semaphore resources
                        if not self.shuffling_buffer_semaphore.decrement(1) or not self.shuffling_io_semaphore.decrement(1):
                            logger.error("Not enough semaphore resources to continue shuffling")
                            self._reset_shuffling_state(partition_to_shuffle)
                            return
                        
                        # Issue a cache-in request
                        addr = f"p{partition_to_shuffle.partition_id}_l{level_id}_pos{position}"
                        logger.info(f"Caching in block from {addr} for shuffling")
                        
                        try:
                            # Use longer timeout for cache-in, with retries
                            cache_result = await with_timeout(
                                self.storage_cache.cache_in(addr, self.server_storage), 
                                timeout_seconds=2,
                                retries=1
                            )
                            if cache_result is not None:
                                cached_blocks.append((addr, cache_result))
                                r += 1
                                logger.info(f"Successfully cached in block {cache_result.block_id} from {addr}")
                            else:
                                # Cache-in failed, increment the semaphores we decremented
                                self.shuffling_buffer_semaphore.increment(1)
                                self.shuffling_io_semaphore.increment(1)
                                logger.warning(f"Failed to cache in block from {addr}")
                        except Exception as e:
                            logger.error(f"Error during cache-in for {addr}: {e}")
                            self.shuffling_buffer_semaphore.increment(1)
                            self.shuffling_io_semaphore.increment(1)
            
            # Decrement shuffling buffer semaphore for additional space
            if w > r:
                logger.info(f"Reserving additional {w - r} slots in shuffling buffer")
                if not self.shuffling_buffer_semaphore.decrement(w - r):
                    logger.error("Not enough shuffling buffer space")
                    self._reset_shuffling_state(partition_to_shuffle)
                    return
                logger.info(f"Reserved additional {w - r} slots in shuffling buffer")
            
            # 3) Perform atomic shuffle
            logger.info(f"Starting atomic shuffle for partition {partition_to_shuffle.partition_id}")
            
            # Fetch cached-in blocks
            all_cached_blocks = []
            for addr, block in cached_blocks:
                if block:
                    all_cached_blocks.append(block)
                    
                    # For early cache-ins, increment the semaphore
                    position_info = self.position_map.get_position(block.block_id)
                    if position_info is None or position_info[0] != partition_to_shuffle.partition_id:
                        # This was an early cache-in
                        self.early_cache_in_semaphore.increment(1)
                        logger.info(f"Incremented early cache-in semaphore for block {block.block_id}")
            
            logger.info(f"Fetched {len(all_cached_blocks)} cached-in blocks for shuffling")
            
            # Fetch blocks from eviction cache - use async version
            logger.info(f"Fetching up to {job_size_snapshot} blocks from eviction cache for partition {partition_to_shuffle.partition_id}")
            try:
                eviction_blocks = await with_timeout(
                    self.eviction_cache.get_blocks_for_partition_async(
                        partition_to_shuffle.partition_id, job_size_snapshot),
                    timeout_seconds=2,
                    retries=1
                )
                
                # Log details about each block from eviction cache
                real_blocks = [b for b in eviction_blocks if not b.is_dummy]
                dummy_blocks = [b for b in eviction_blocks if b.is_dummy]
                
                logger.info(f"Retrieved {len(real_blocks)} real blocks from eviction cache for partition {partition_to_shuffle.partition_id}")
                for block in real_blocks:
                    logger.info(f"  Real block {block.block_id} with value '{block.value}'")
                
                logger.info(f"Added {len(dummy_blocks)} dummy blocks to pad eviction")
            except Exception as e:
                logger.warning(f"Error retrieving blocks from eviction cache: {e}")
                logger.warning("Falling back to sync version")
                eviction_blocks = self.eviction_cache.get_blocks_for_partition(
                    partition_to_shuffle.partition_id, job_size_snapshot)
            
            real_eviction_blocks = [b for b in eviction_blocks if not b.is_dummy]
            dummy_eviction_blocks = [b for b in eviction_blocks if b.is_dummy]
            logger.info(f"Got {len(real_eviction_blocks)} real blocks and {len(dummy_eviction_blocks)} dummy blocks from eviction cache")
            
            # Increment eviction semaphore accordingly
            self.eviction_semaphore.increment(job_size_snapshot)
            logger.info(f"Incremented eviction semaphore by {job_size_snapshot}")
            
            # Add blocks to shuffling buffer
            self.shuffling_buffer.add_multiple_blocks(all_cached_blocks)
            logger.info(f"Added {len(all_cached_blocks)} cached-in blocks to shuffling buffer")
            self.shuffling_buffer.add_multiple_blocks(eviction_blocks)
            logger.info(f"Added {len(eviction_blocks)} blocks from eviction cache to shuffling buffer")
            
            # Pad buffer to size w
            padding_blocks = w - len(all_cached_blocks) - len(eviction_blocks)
            if padding_blocks > 0:
                logger.info(f"Padding shuffling buffer with {padding_blocks} dummy blocks")
                self.shuffling_buffer.pad_to_size(w)
            
            # Permute the shuffling buffer
            self.shuffling_buffer.permute()
            logger.info(f"Shuffled {w} blocks for partition {partition_to_shuffle.partition_id}")
            
            # Use a set to track used positions in each level to avoid conflicts
            used_positions = {level_id: set() for level_id in range(len(partition_to_shuffle.levels))}
            
            # Store shuffled blocks into storage cache
            for level_id in range(len(partition_to_shuffle.levels)):
                if (new_counter >> level_id) & 1:  # This level should be filled
                    level_size = 2 ** level_id
                    blocks_for_level = self.shuffling_buffer.get_blocks(2 * level_size)
                    logger.info(f"Storing {len(blocks_for_level)} blocks to level {level_id} of partition {partition_to_shuffle.partition_id}")
                    
                    real_blocks = 0
                    dummy_blocks = 0
                    
                    # Store blocks and update position map
                    for i, block in enumerate(blocks_for_level):
                        # Skip if position is already used (find another position)
                        if i in used_positions[level_id]:
                            for alt_pos in range(2 * level_size):
                                if alt_pos not in used_positions[level_id]:
                                    i = alt_pos
                                    break
                        
                        used_positions[level_id].add(i)
                        
                        if not block.is_dummy:
                            real_blocks += 1
                            addr = f"p{partition_to_shuffle.partition_id}_l{level_id}_pos{i}"
                            self.storage_cache.store(addr, block)
                            
                            # Update position map - remove old position and set new one
                            old_position = self.position_map.get_position(block.block_id)
                            if old_position:
                                logger.info(f"Updating position for block {block.block_id} from {old_position} to " + 
                                        f"({partition_to_shuffle.partition_id}, {level_id}, {i})")
                            else:
                                logger.info(f"Setting new position for block {block.block_id}: " + 
                                         f"({partition_to_shuffle.partition_id}, {level_id}, {i})")
                            
                            # Set the new position
                            self.position_map.set_position(block.block_id, partition_to_shuffle.partition_id, level_id, i)
                            
                            # Remove from pending blocks if it was in eviction cache
                            if hasattr(self.eviction_cache, 'pending_blocks') and block.block_id in self.eviction_cache.pending_blocks:
                                with self.eviction_cache.lock:  # Use lock to avoid race conditions
                                    if block.block_id in self.eviction_cache.pending_blocks:
                                        self.eviction_cache.pending_blocks.remove(block.block_id)
                                        logger.info(f"Removed block {block.block_id} from pending blocks in eviction cache")
                            
                            logger.info(f"Stored block {block.block_id} at {addr}")
                        else:
                            dummy_blocks += 1
                    
                    logger.info(f"Level {level_id} filled with {real_blocks} real blocks and {dummy_blocks} dummy blocks")
            
            # Update partition counter and clear shuffling state
            logger.info(f"Updating partition counter from {current_counter} to {new_counter}")
            partition_to_shuffle.counter = new_counter
            for i in range(len(partition_to_shuffle.levels)):
                partition_to_shuffle.marked_for_shuffling[i] = False
            
            # Clean up partition tracking
            with self.shuffling_lock:
                partition_to_shuffle.being_shuffled = False
                if partition_to_shuffle.partition_id in self.partitions_being_shuffled:
                    self.partitions_being_shuffled.remove(partition_to_shuffle.partition_id)
            
            logger.info(f"Completed shuffle for partition {partition_to_shuffle.partition_id}, new counter: {new_counter}")
            
            # 4) Cache-out in the background
            logger.info(f"Starting cache-out operations for partition {partition_to_shuffle.partition_id}")
            cache_out_tasks = []
            for level_id in range(len(partition_to_shuffle.levels)):
                if (new_counter >> level_id) & 1:  # This level is filled
                    level_size = 2 ** level_id
                    for i in range(2 * level_size):
                        # Decrement shuffling I/O semaphore
                        if not self.shuffling_io_semaphore.decrement(1):
                            logger.warning(f"Not enough I/O semaphore resources for cache-out at level {level_id}, pos {i}")
                            self.shuffling_io_semaphore.increment(1)  # Undo decrement
                            continue
                        
                        # Issue cache-out with timeout protection
                        addr = f"p{partition_to_shuffle.partition_id}_l{level_id}_pos{i}"
                        task = asyncio.create_task(self._safe_cache_out(addr))
                        cache_out_tasks.append(task)
            
            logger.info(f"Scheduled {len(cache_out_tasks)} cache-out operations")
            
            # Don't wait for cache-out operations - let them complete in the background
            if cache_out_tasks:
                asyncio.create_task(self._wait_for_cache_out_tasks(cache_out_tasks))
        
        except Exception as e:
            logger.error(f"Error in background shuffler: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            
            # Reset state for the partition being shuffled
            if 'partition_to_shuffle' in locals() and partition_to_shuffle:
                self._reset_shuffling_state(partition_to_shuffle)
                
            # Make sure semaphores are properly restored to prevent deadlock
            try:
                self.shuffling_buffer_semaphore.increment(1)
                self.shuffling_io_semaphore.increment(1)
                logger.info("Incremented semaphores after error to prevent deadlock")
            except Exception as release_error:
                logger.error(f"Error releasing semaphores: {release_error}")

    async def _wait_for_cache_out_tasks(self, tasks):
        """Wait for cache-out tasks to complete in the background"""
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error waiting for cache-out tasks: {e}")

    async def _safe_cache_out(self, addr):
        """Safe wrapper for cache-out operations with proper semaphore handling"""
        try:
            success = await with_timeout(
                self.storage_cache.cache_out(addr, self.server_storage), 
                timeout_seconds=2,
                retries=1
            )
            # Always increment the shuffling buffer semaphore, regardless of success
            self.shuffling_buffer_semaphore.increment(1)
            logger.info(f"Cached out block at {addr}, incremented shuffling buffer semaphore")
        except Exception as e:
            logger.error(f"Error during cache-out for {addr}: {e}")
            # Make sure we increment the semaphore even on error
            self.shuffling_buffer_semaphore.increment(1)
            logger.info(f"Incremented shuffling buffer semaphore after cache-out error")

    def _reset_shuffling_state(self, partition):
        """Reset the shuffling state for a partition after an error"""
        with self.shuffling_lock:
            partition.being_shuffled = False
            for i in range(len(partition.levels)):
                partition.marked_for_shuffling[i] = False
            if partition.partition_id in self.partitions_being_shuffled:
                self.partitions_being_shuffled.remove(partition.partition_id)
        logger.info(f"Reset shuffling state for partition {partition.partition_id}")

class ORAMMain:
    """
    Main ORAM handler - entry point for client operations
    """
    def __init__(self, num_partitions: int, partition_capacity: int, eviction_rate: float = 2.0):
        # Initialize data structures
        self.partitions = []
        for i in range(num_partitions):
            self.partitions.append(Partition(i, partition_capacity))
        
        self.position_map = PositionMap()
        self.eviction_cache = EvictionCache()
        self.storage_cache = StorageCache()
        self.shuffling_buffer = ShufflingBuffer()
        self.server_storage = ServerStorage()
        
        # Initialize semaphores with higher initial values for safety
        self.early_cache_in_semaphore = Semaphore(1000, "early_cache_in")
        self.shuffling_buffer_semaphore = Semaphore(1000, "shuffling_buffer")
        self.eviction_semaphore = Semaphore(1000, "eviction")
        self.shuffling_io_semaphore = Semaphore(1000, "shuffling_io")
        
        # Initialize components
        self.partition_reader = PartitionReader(
            self.storage_cache, 
            self.server_storage, 
            self.position_map, 
            self.early_cache_in_semaphore
        )
        
        self.background_shuffler = BackgroundShuffler(
            self.partitions, 
            self.storage_cache, 
            self.server_storage, 
            self.position_map, 
            self.eviction_cache,
            self.shuffling_buffer,
            self.early_cache_in_semaphore,
            self.shuffling_buffer_semaphore,
            self.eviction_semaphore,
            self.shuffling_io_semaphore
        )
        
        self.eviction_rate = eviction_rate
        self.operation_lock = threading.RLock()  # Lock for coordinating operations
        
        # Initialize with some blocks for testing
        self.initialized = False
        
        # Event loop and tasks
        self.loop = asyncio.new_event_loop()
        self.shuffler_task = None
        
    async def initialize(self, num_blocks: int):
        """Initialize ORAM with dummy blocks"""
        if self.initialized:
            return
        
        # Add blocks to storage
        logger.info(f"Initializing ORAM with {num_blocks} blocks")
        
        # For each partition, track used positions by level
        partition_used_positions = [{} for _ in range(len(self.partitions))]
        
        # Evenly distribute blocks among partitions
        for block_id in range(num_blocks):
            # Create block and assign to random partition
            block = Block(block_id)
            partition_id = random.randint(0, len(self.partitions) - 1)
            partition = self.partitions[partition_id]
            
            # Initialize used positions for this partition if not done already
            if not partition_used_positions[partition_id]:
                partition_used_positions[partition_id] = {i: set() for i in range(len(partition.levels))}
            
            # Choose a random level and make sure it's marked as filled
            level_id = random.randint(0, len(partition.levels) - 1)
            while not (partition.counter & (1 << level_id)):
                # Ensure level is filled (according to counter)
                partition.counter |= (1 << level_id)
            
            level = partition.levels[level_id]
            
            # Try to find an unused position, trying multiple levels and partitions if needed
            success = False
            retry_attempts = 0
            # Make max retries proportional to the number of blocks to handle larger test cases
            max_retries = max(100, num_blocks // 10)  # Dynamically scale based on block count

            while not success and retry_attempts < max_retries:
                # Choose an unused position in this level
                used_pos = partition_used_positions[partition_id][level_id]
                available_pos = set(range(len(level))) - used_pos
                
                if available_pos:
                    success = True
                    break
                    
                # Try another level in the same partition
                if retry_attempts < max_retries // 2:
                    level_id = random.randint(0, len(partition.levels) - 1)
                    level = partition.levels[level_id]
                # If still failing, try another partition entirely
                else:
                    partition_id = random.randint(0, len(self.partitions) - 1)
                    partition = self.partitions[partition_id]
                    
                    # Initialize used positions for this partition if not done already
                    if not partition_used_positions[partition_id]:
                        partition_used_positions[partition_id] = {i: set() for i in range(len(partition.levels))}
                        
                    level_id = random.randint(0, len(partition.levels) - 1)
                    level = partition.levels[level_id]
                
                # Ensure level is marked as filled (according to counter)
                if not (partition.counter & (1 << level_id)):
                    partition.counter |= (1 << level_id)
                    
                retry_attempts += 1

            if not success:
                logger.warning(f"Failed to find space for block {block_id}. Skipping.")
                continue

            # At this point, we have a valid position
            position = random.choice(list(available_pos))
            partition_used_positions[partition_id][level_id].add(position)
            
            # Update position map
            self.position_map.set_position(block_id, partition_id, level_id, position)
            
            # Store block on server
            addr = f"p{partition_id}_l{level_id}_pos{position}"
            await self.server_storage.write(addr, block)
            
            logger.info(f"Initialized block {block_id} in partition {partition_id}, level {level_id}, position {position}")
        
        self.initialized = True
        logger.info("ORAM initialization complete")
    
    async def start(self):
        """Start the background shuffler"""
        # Start background shuffler in a separate task
        if self.shuffler_task is None:
            self.shuffler_task = asyncio.create_task(self.background_shuffler.start())
            logger.info("Background shuffler started")

    async def stop(self):
        """Stop the background shuffler"""
        if self.shuffler_task:
            self.background_shuffler.stop()
            try:
                # Force timeout the shuffler task to prevent hanging
                await with_timeout(self.shuffler_task, timeout_seconds=2, retries=1)
            except Exception as e:
                logger.warning(f"Forced background shuffler task termination: {e}")
            finally:
                if not self.shuffler_task.done():
                    self.shuffler_task.cancel()
                self.shuffler_task = None
                logger.info("Background shuffler stopped")
    
    async def safe_operation(self, func, *args, **kwargs):
        """Execute an ORAM operation with retries and better error handling"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return await func(*args, **kwargs)
            except asyncio.TimeoutError:
                logger.warning(f"Operation timed out, retrying ({attempt+1}/{max_retries})")
                # Short delay before retry
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Operation failed: {e}")
                import traceback
                logger.error(f"Stack trace: {traceback.format_exc()}")
                # Short delay before retry
                await asyncio.sleep(0.5)
        
        # If we get here, all retries failed
        logger.error(f"Operation failed after {max_retries} retries")
        raise RuntimeError(f"ORAM operation failed after {max_retries} retries")
    
    async def _acquire_operation_lock(self, client_id, operation, block_id):
        """
        Simple operation coordination to prevent conflicts between multiple clients
        """
        await asyncio.sleep(client_id * 0.1)  # Stagger client operations slightly
        logger.info(f"Client {client_id} starting {operation} for block {block_id}")
    
    async def read(self, block_id: int, client_id: int = 0) -> Tuple[Block, int]:
        """
        Read a block from ORAM with better error handling
        """
        return await self.safe_operation(self._read_impl, block_id, client_id)
    
    async def _read_impl(self, block_id: int, client_id: int = 0) -> Tuple[Block, int]:
        """
        Read a block from ORAM (implements Figure 5 algorithm)
        Returns the block and the partition_id it was read from
        """
        await self._acquire_operation_lock(client_id, "read", block_id)
        logger.info(f"Read request for block {block_id} from client {client_id}")
        
        # Log the initial state of the eviction cache
        cache_status = self.eviction_cache.get_cache_status()
        logger.info(f"Eviction cache before read: {cache_status['total_blocks']} blocks total")
        for p_id, count in cache_status['partition_counts'].items():
            logger.info(f"  Partition {p_id}: {count} blocks")
        
        # Check if the block is in the eviction cache first
        with self.eviction_cache.lock:
            for bid, (block, partition_id) in self.eviction_cache.cache.items():
                if bid == block_id:
                    logger.info(f"Block {block_id} found in eviction cache, assigned to partition {partition_id}")
                    return block, partition_id
        
        # Decrement early cache-in semaphore by number of levels 
        # (will be incremented later by partition reader for each qualifying level)
        total_levels = sum(len(partition.levels) for partition in self.partitions)
        self.early_cache_in_semaphore.decrement(total_levels)
        logger.info(f"Decremented early cache-in semaphore by {total_levels} (for read)")
        
        # Decrement eviction semaphore by eviction rate
        self.eviction_semaphore.decrement(self.eviction_rate)
        logger.info(f"Decremented eviction semaphore by {self.eviction_rate} (for read)")
        
        # Lookup position map to determine which partition the block is in
        position_info = self.position_map.get_position(block_id)
        
        if position_info:
            partition_id, level_id, position = position_info
            logger.info(f"Block {block_id} found in position map: partition {partition_id}, level {level_id}, position {position}")
            
            # Read the block from the partition
            partition = self.partitions[partition_id]
            block = await with_timeout(
                self.partition_reader.read_partition(partition, block_id),
                timeout_seconds=2,
                retries=1
            )
            
            if block:
                # Store block to eviction cache and choose a new random partition
                new_partition_id = random.randint(0, len(self.partitions) - 1)
                logger.info(f"Read successful. Assigning block {block_id} to new partition {new_partition_id} in eviction cache")
                await with_timeout(
                    self.eviction_cache.add_block_async(block, new_partition_id),
                    timeout_seconds=1,
                    retries=1
                )
                
                # NOTE: We no longer remove from position map here
                # The position will be updated during the shuffling process
                
                # Choose random partitions for eviction
                self._perform_evictions(client_id, block_id)
                
                # Log the state of the eviction cache after eviction
                cache_status = self.eviction_cache.get_cache_status()
                logger.info(f"Eviction cache after read and eviction: {cache_status['total_blocks']} blocks total")
                for p_id, count in cache_status['partition_counts'].items():
                    logger.info(f"  Partition {p_id}: {count} blocks")
                
                return block, partition_id
            else:
                logger.warning(f"Block {block_id} not found in partition {partition_id} despite position map entry")
                
                # Choose random partitions for eviction anyway
                self._perform_evictions(client_id, block_id)
                
                # Log the state of the eviction cache after eviction
                cache_status = self.eviction_cache.get_cache_status()
                logger.info(f"Eviction cache after read and eviction: {cache_status['total_blocks']} blocks total")
                for p_id, count in cache_status['partition_counts'].items():
                    logger.info(f"  Partition {p_id}: {count} blocks")
                
                # Return dummy block
                return Block(-1, f"Dummy block for {block_id}", True), partition_id
        else:
            logger.warning(f"Block {block_id} not found in position map")
            
            # Choose a random partition to read from (as if the block were there)
            random_partition_id = random.randint(0, len(self.partitions) - 1)
            partition = self.partitions[random_partition_id]
            
            # Make a dummy read to prevent information leakage
            await with_timeout(
                self.partition_reader.read_partition(partition, -1),  # Dummy block_id
                timeout_seconds=2,
                retries=1
            )
            
            # Choose random partitions for eviction
            self._perform_evictions(client_id, block_id)
            
            # Log the state of the eviction cache after eviction
            cache_status = self.eviction_cache.get_cache_status()
            logger.info(f"Eviction cache after read and eviction: {cache_status['total_blocks']} blocks total")
            for p_id, count in cache_status['partition_counts'].items():
                logger.info(f"  Partition {p_id}: {count} blocks")
            
            # Return dummy block
            return Block(-1, f"Dummy block for {block_id}", True), random_partition_id
    
    async def write(self, block_id: int, value: Any, client_id: int = 0) -> Tuple[bool, int]:
        """
        Write a block to ORAM with better error handling
        """
        return await self.safe_operation(self._write_impl, block_id, value, client_id)
    
    async def _write_impl(self, block_id: int, value: Any, client_id: int = 0) -> Tuple[bool, int]:
        """
        Write a block to ORAM
        Returns success status and the partition_id it was read from
        """
        await self._acquire_operation_lock(client_id, "write", block_id)
        logger.info(f"Write request for block {block_id} with value {value} from client {client_id}")
        
        # Log the initial state of the eviction cache
        cache_status = self.eviction_cache.get_cache_status()
        logger.info(f"Eviction cache before write: {cache_status['total_blocks']} blocks total")
        for p_id, count in cache_status['partition_counts'].items():
            logger.info(f"  Partition {p_id}: {count} blocks")
        
        # Reading the block first is similar to read operation
        block, partition_id = await self._read_impl(block_id, client_id)
        
        # Update block value
        if block and not block.is_dummy:
            block.value = value
            logger.info(f"Updated block {block_id} with new value: {value}")
            
            # Log the state of the eviction cache after write
            cache_status = self.eviction_cache.get_cache_status()
            logger.info(f"Eviction cache after write: {cache_status['total_blocks']} blocks total")
            for p_id, count in cache_status['partition_counts'].items():
                logger.info(f"  Partition {p_id}: {count} blocks")
            
            return True, partition_id
        else:
            # Block doesn't exist, create a new one
            new_block = Block(block_id, value)
            
            # Choose a random partition for the new block
            new_partition_id = random.randint(0, len(self.partitions) - 1)
            
            # Add to eviction cache
            await with_timeout(
                self.eviction_cache.add_block_async(new_block, new_partition_id),
                timeout_seconds=1,
                retries=1
            )
            logger.info(f"Created new block {block_id} with value {value}, assigned to partition {new_partition_id}")
            
            # Log the state of the eviction cache after write
            cache_status = self.eviction_cache.get_cache_status()
            logger.info(f"Eviction cache after write: {cache_status['total_blocks']} blocks total")
            for p_id, count in cache_status['partition_counts'].items():
                logger.info(f"  Partition {p_id}: {count} blocks")
            
            return True, partition_id
    
    def _perform_evictions(self, client_id: int, block_id: int):
        """
        Choose random partitions for eviction (part of read/write operations)
        """
        # Choose ν partitions at random for eviction
        eviction_count = int(self.eviction_rate)
        if random.random() < (self.eviction_rate - eviction_count):
            eviction_count += 1
            
        logger.info(f"Client {client_id} performing {eviction_count} evictions after accessing block {block_id}")
        
        selected_partitions = []
        # Randomly select partitions for eviction and increment their job sizes
        for i in range(eviction_count):
            partition_id = random.randint(0, len(self.partitions) - 1)
            partition = self.partitions[partition_id]
            partition.job_size += 1
            selected_partitions.append(partition_id)
            logger.info(f"Eviction {i+1}/{eviction_count}: Scheduled eviction for partition {partition_id}, new job size: {partition.job_size}")
        
        logger.info(f"Eviction complete. Selected partitions: {selected_partitions}")


class ObliviStoreClient:
    """
    Client interface for ObliviStore
    """
    def __init__(self, client_id: int, oram: ORAMMain):
        self.client_id = client_id
        self.oram = oram
        logger.info(f"Client {client_id} initialized")
    
    async def read(self, block_id: int) -> Tuple[Any, int]:
        """
        Read a block from ORAM
        Returns the block value and the partition it was read from
        """
        start_time = time.time()
        block, partition_id = await self.oram.read(block_id, self.client_id)
        end_time = time.time()
        
        response_time = (end_time - start_time) * 1000  # Convert to ms
        logger.info(f"Client {self.client_id} read block {block_id} from partition {partition_id} in {response_time:.2f}ms")
        
        return block.value if block and not block.is_dummy else None, partition_id
    
    async def write(self, block_id: int, value: Any) -> Tuple[bool, int]:
        """
        Write a block to ORAM
        Returns success status and the partition it was written to
        """
        start_time = time.time()
        success, partition_id = await self.oram.write(block_id, value, self.client_id)
        end_time = time.time()
        
        response_time = (end_time - start_time) * 1000  # Convert to ms
        logger.info(f"Client {self.client_id} wrote block {block_id} through partition {partition_id} in {response_time:.2f}ms")
        
        return success, partition_id


class ObliviStoreSystem:
    """
    Main system class that manages the ORAM and clients
    """
    def __init__(self, num_partitions: int, partition_capacity: int, eviction_rate: float = 2.0):
        self.oram = ORAMMain(num_partitions, partition_capacity, eviction_rate)
        self.clients = {}
        self.loop = asyncio.get_event_loop()
    
    async def initialize(self, num_blocks: int):
        """Initialize the system"""
        
        await self.oram.initialize(num_blocks)

        try:
            await with_timeout(self.oram.start(), timeout_seconds=2)
            logger.info("Background shuffler started during initialization")
        except Exception as e:
            logger.warning(f"Background shuffler failed to start: {e}")

        logger.info(f"ObliviStore system initialized with {num_blocks} blocks")
    
    def add_client(self, client_id: int) -> ObliviStoreClient:
        """Add a new client to the system"""
        if client_id in self.clients:
            logger.warning(f"Client {client_id} already exists")
            return self.clients[client_id]
        
        client = ObliviStoreClient(client_id, self.oram)
        self.clients[client_id] = client
        logger.info(f"Added client {client_id} to the system")
        return client
    
    def remove_client(self, client_id: int) -> bool:
        """Remove a client from the system"""
        if client_id in self.clients:
            del self.clients[client_id]
            logger.info(f"Removed client {client_id} from the system")
            return True
        return False
    
    async def shutdown(self):
        """Shutdown the system"""
        await self.oram.stop()
        logger.info("ObliviStore system shut down")
    
    async def process_query(self, client_id: int, query: str) -> str:
        """
        Process a query from a client
        Query format: 
        - 'R <block_id>' for read
        - 'W <block_id> <value>' for write
        """
        if client_id not in self.clients:
            client = self.add_client(client_id)
        else:
            client = self.clients[client_id]
        
        parts = query.strip().split()
        
        if len(parts) < 2:
            return "Error: Invalid query format"
        
        operation = parts[0].upper()
        
        try:
            block_id = int(parts[1])
            
            if operation == 'R':  # Read operation
                value, partition_id = await client.read(block_id)
                if value is not None:
                    return f"Partition: P{partition_id}, Value: {value}, Read of block {block_id} is successful."
                else:
                    return f"Partition: P{partition_id}, Read of block {block_id} failed (block not found)."
                
            elif operation == 'W':  # Write operation
                if len(parts) < 3:
                    return "Error: Write operation requires a value"
                
                value = ' '.join(parts[2:])  # Join the rest of the parts as the value
                success, partition_id = await client.write(block_id, value)
                
                if success:
                    return f"Partition: P{partition_id}, Acknowledgement: Write of block {block_id} is successful."
                else:
                    return f"Partition: P{partition_id}, Write of block {block_id} failed."
            
            else:
                return f"Error: Unknown operation '{operation}'"
                
        except ValueError:
            return "Error: Block ID must be an integer"
        except Exception as e:
            logger.exception("Error processing query")
            return f"Error: {str(e)}"


async def process_query_file(system: ObliviStoreSystem, client_id: int, query_file: str):
    """
    Process queries from a file for a specific client
    """
    try:
        with open(query_file, 'r') as f:
            queries = f.readlines()
        
        logger.info(f"Processing {len(queries)} queries for client {client_id} from file {query_file}")
        
        results = []
        for query in queries:
            query = query.strip()
            if not query or query.startswith('#'):
                continue
                
            logger.info(f"Client {client_id} executing query: {query}")
            result = await system.process_query(client_id, query)
            results.append(result)
            logger.info(f"Result: {result}")
        
        return results
    
    except Exception as e:
        logger.exception(f"Error processing query file {query_file}")
        return [f"Error: {str(e)}"]


async def main_interactive():
    """
    Run the system in interactive mode
    """
    print("ObliviStore System - Interactive Mode")
    
    # Get system parameters
    num_partitions = int(input("Enter number of partitions: ") or "4")
    num_blocks = int(input("Enter number of blocks to initialize: ") or "16")
    
    # Check if the number of blocks exceeds partitions^2 

    if num_blocks > num_partitions**2:
        print(f"WARNING: Unexpected Input! Number of blocks ({num_blocks}) exceeds the recommended limit of partitions^2 ({num_partitions}^2 = {num_partitions**2}).")
        print(f"This may result in additional dummy block usage and unexpected results.")

    # Auto-calculate partition capacity
    per_partition_blocks = math.ceil(num_blocks / num_partitions)
    
    # Find minimum level count L such that sum(2^i for i in range(L)) >= per_partition_blocks
    L = 1
    while (2 ** L - 1) < per_partition_blocks:
        L += 1
    
    # Partition capacity is 2^(2L), as required by the ORAM partition level design
    partition_capacity = 2 ** (2 * L)
    # print(f"Auto-calculated partition capacity: {partition_capacity}")
    
    # Create and initialize the system
    system = ObliviStoreSystem(num_partitions, partition_capacity)
    print("Initializing system...")
    await system.initialize(num_blocks)
    print("System initialized")
    
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


async def main_batch(config_file=None):
    """
    Run the system in batch mode with a configuration file
    """
    if config_file is None:
        config_file = "oblivistore_config.json"
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except:
        print(f"Config file {config_file} not found or invalid. Using defaults.")
        config = {
            "num_partitions": 4,
            "partition_capacity": 8,
            "num_blocks": 16,
            "num_clients": 2,
            "query_files": ["client1_queries.txt", "client2_queries.txt"]
        }
    
    # Create and initialize the system
    system = ObliviStoreSystem(config["num_partitions"], config["partition_capacity"])
    print("Initializing system...")
    await system.initialize(config["num_blocks"])
    print("System initialized")
    
    # Create clients
    clients = []
    for i in range(config["num_clients"]):
        client = system.add_client(i)
        clients.append(client)
    
    # Process query files in parallel
    tasks = []
    for i, query_file in enumerate(config["query_files"]):
        if i < len(clients):
            task = asyncio.create_task(process_query_file(system, i, query_file))
            tasks.append(task)
    
    # Wait for all tasks to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Print results
    for i, client_results in enumerate(results):
        if isinstance(client_results, Exception):
            print(f"Error for client {i}: {client_results}")
        else:
            print(f"Results for client {i}:")
            for j, result in enumerate(client_results):
                print(f"  Query {j+1}: {result}")
    
    # Shutdown
    print("Shutting down system...")
    await system.shutdown()
    print("System shut down")


def main():
    """
    Main entry point
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="ObliviStore System")
    parser.add_argument("--batch", action="store_true", help="Run in batch mode")
    parser.add_argument("--config", help="Configuration file for batch mode")
    args = parser.parse_args()
    
    if args.batch:
        asyncio.run(main_batch(args.config))
    else:
        asyncio.run(main_interactive())


if __name__ == "__main__":
    main()