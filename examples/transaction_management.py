from shard_lite import create_shard_manager, Config

# Step 1: Initialize configuration
config = Config(
    connection_timeout=30,
    pool_size=5,
    shard_base_path="./shards"
)

# Step 2: Create a ShardManager instance with the 'hash' strategy
shard_manager = create_shard_manager(config, strategy='hash')

# Step 3: Create shards
shard_manager.create_shard()
shard_manager.create_shard()

# Step 4: Define a transaction with multiple operations
operations = [
    {"type": "insert", "params": {"data": {"id": 1, "name": "Alice"}}},
    {"type": "insert", "params": {"data": {"id": 2, "name": "Bob"}}},
    {"type": "update", "params": {"criteria": {"id": 1}, "data": {"name": "Alice Updated"}}}
]

# Step 5: Execute the transaction
try:
    shard_manager.execute_transaction(operations)
    print("Transaction executed successfully.")
except Exception as e:
    print("Transaction failed:", str(e))

# Step 6: Query data to verify transaction
result = shard_manager.select({"id": 1})
print("Transaction Query Result:", result)

# Step 7: Clean up resources
shard_manager.close()
