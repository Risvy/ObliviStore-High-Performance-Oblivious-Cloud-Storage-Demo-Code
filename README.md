# ObliviStore-High-Performance-Oblivious-Cloud-Storage-Demo-Code
## WARNING: This code is not tested, might contain bugs, and might not fully reflect the original implementation of the Oblivistore framework. This code is still incomplete. 

## A reference code to work on for the paper titled "ObliviStore: High Performance Oblivious Cloud Storage". This code is still incomplete. As the main code of this paper was not available at the time of this writing, this was an attempt to present a base on which future developers can work. 

# How to Run the Project

## Prerequisites

- Python 3.7+ installed
- No additional libraries required (project uses standard Python libraries only)

## Running the Interactive Mode

1. **Clone or download the repository**
2. **Navigate to the project directory**
   ```bash
   cd ObliviStore
   ```
3. **Run the main implementation file**
   ```bash
   python oblivistore_implementation.py
   ```
4. **When prompted, enter:**
   - Number of partitions (recommended: at least √N, where N is your block count)
   - Number of blocks to initialize

5. **Use the following commands in the interactive prompt:**
   - `R <block_id>` - Read a block
   - `W <block_id> <value>` - Write a value to a block
   - `client <id>` - Switch to a different client
   - `file <filename>` - Process commands from a file
   - `stats` - Show system statistics
   - `help` - Display all available commands
   - `exit` - Exit the program

## Example Usage

```bash
# Start in interactive mode
python oblivistore_implementation.py

# Enter parameters
Enter number of partitions: 8
Enter number of blocks to initialize: 64

# At the prompt, try:
> W 3 test_value_123
> R 3
> exit
```

---

# Test Cases

## Test Case 1: Basic Read-After-Write

Run the ORAM system interactively:

```bash
python oblivistore_implementation.py
```

When prompted, enter:
- Number of partitions: 8
- Number of blocks to initialize: 16

Then at the command prompt:
```
W 3 test_value_123
R 3
```

*(Read block 3 to verify the write succeeded)*

You should see the value "test_value_123" returned in the read result, confirming the read-after-write works.

## Test Case 2: Multiple Writes and Reads to Same Block

```bash
python oblivistore_implementation.py
```

When prompted, enter:
- Number of partitions: 8
- Number of blocks to initialize: 16

Then at the command prompt:
```
W 5 first_value
W 5 second_value
R 5  # Should return "second_value"
W 5 third_value
R 5  # Should return "third_value"
```

This tests that multiple writes to the same block work correctly, and reads always return the most recent value.

## Test Case 3: Interleaved Reads and Writes to Different Blocks

```bash
python oblivistore_implementation.py
```

When prompted, enter:
- Number of partitions: 8
- Number of blocks to initialize: 16

Then at the command prompt:
```
W 7 value_for_block_7
W 9 value_for_block_9
R 7  # Should return "value_for_block_7"
W 11 value_for_block_11
R 9  # Should return "value_for_block_9"
R 11  # Should return "value_for_block_11"
```

This tests that writing to one block doesn't affect other blocks, and the system can track multiple blocks correctly.

## Test Case 4: Multiple Clients (Using the "client" Command)

```bash
python oblivistore_implementation.py
```

When prompted, enter:
- Number of partitions: 8
- Number of blocks to initialize: 16

Then at the command prompt:
```
client 1  # Switch to client 1
W 4 client1_value
client 2  # Switch to client 2
R 4  # Should return "client1_value"
W 4 client2_value
client 1  # Switch back to client 1
R 4  # Should return "client2_value"
```

This tests that multiple clients can read and write to the same blocks correctly.

## Test Case 5: Multiple Clients Writing and Reading Across Shared and Unique Blocks

```bash
python oblivistore_implementation.py
```

Parameters:
- Number of partitions: 8
- Number of blocks to initialize: 16

Commands:
```
client 0
W 3 client0_data
R 3

client 1
W 3 client1_data
R 3

client 0
R 3

client 0
W 4 client0_block4

client 1
W 5 client1_block5

client 2
W 6 client2_block6

client 0
R 4

client 1
R 5

client 2
R 6
```

**This sequence does the following:**
- Client 0 writes to block 3, then Client 1 overwrites it. Reading it again from either client confirms visibility.
- Each client writes to a different block and verifies that reads work independently.

## Test Case 6: Multiple Clients Writing and Reading Across Shared and Unique Blocks (with different partitions)

```bash
python oblivistore_implementation.py
```

Parameters:
- Number of partitions: 6
- Number of blocks to initialize: 18

Commands:
```
client 0
W 3 client0_data
R 3

client 1
W 3 client1_data
R 3

client 0
R 3

client 0
W 4 client0_block4

client 1
W 5 client1_block5

client 2
W 6 client2_block6

client 0
R 4

client 1
R 5

client 2
R 6
```

**This sequence does the following:**
- Client 0 writes to block 3, then Client 1 overwrites it. Reading it again from either client confirms visibility.
- Each client writes to a different block and verifies that reads work independently.
