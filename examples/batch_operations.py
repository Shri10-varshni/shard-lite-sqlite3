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

# Step 4: Insert multiple records in batch
batch_data = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
    {"id": 3, "name": "Charlie"}
]
shard_manager.insert(batch_data, handler_type='batch')

# Step 5: Query multiple records
criteria = [{"id": 1}, {"id": 2}, {"id": 3}]
results = shard_manager.select(criteria, handler_type='batch')
print("Batch Query Results:", results)

# Step 6: Update multiple records
update_criteria = [{"id": 1}, {"id": 2}]
update_data = [{"name": "Alice Updated"}, {"name": "Bob Updated"}]
shard_manager.update(update_criteria, update_data, handler_type='batch')

# Step 7: Delete multiple records
delete_criteria = [{"id": 1}, {"id": 2}]
shard_manager.delete(delete_criteria, handler_type='batch')

# Step 8: Clean up resources
shard_manager.close()
