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
shard1 = shard_manager.create_shard()
shard2 = shard_manager.create_shard()

# Step 4: Insert data
data = {"id": 1, "name": "Alice"}
shard_manager.insert(data)

# Step 5: Query data
result = shard_manager.select({"id": 1})
print("Query Result:", result)

# Step 6: Update data
shard_manager.update({"id": 1}, {"name": "Alice Updated"})

# Step 7: Query updated data
updated_result = shard_manager.select({"id": 1})
print("Updated Result:", updated_result)

# Step 8: Delete data
shard_manager.delete({"id": 1})

# Step 9: Verify deletion
deleted_result = shard_manager.select({"id": 1})
print("Deleted Result:", deleted_result)

# Step 10: Clean up resources
shard_manager.close()
