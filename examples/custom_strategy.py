from shard_lite import create_shard_manager, Config

# Step 1: Initialize configuration
config = Config(
    connection_timeout=30,
    pool_size=5,
    directory_path="./directory.json"
)

# Step 2: Create a ShardManager instance with the 'directory' strategy
shard_manager = create_shard_manager(config, strategy='directory')

# Step 3: Add custom key-to-shard mappings
directory_strategy = shard_manager.strategy
directory_strategy.add_mapping("key1", "shard_1")
directory_strategy.add_mapping("key2", "shard_2")

# Step 4: Query shard for a specific key
shard_id = directory_strategy.get_shard_for_key("key1")
print("Shard for key1:", shard_id)

# Step 5: Insert data into the mapped shard
shard_manager.insert({"id": "key1", "name": "Alice"})

# Step 6: Query data
result = shard_manager.select({"id": "key1"})
print("Query Result:", result)

# Step 7: Clean up resources
shard_manager.close()
