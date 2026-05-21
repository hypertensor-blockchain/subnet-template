"""Test script to populate RocksDB with sample data and test the API."""

from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from subnet.utils.db.database import RocksDB

# python -m pytest tests/api/test_populate_db.py


def populate_test_data():
    """Populate RocksDB with sample test data."""
    print("Creating test database...")

    # Create database
    db = RocksDB(base_path="/tmp/test_rocksdb")

    # Add some simple key-value pairs
    print("Adding simple key-value pairs...")
    db.set("test_key_1", "value_1")
    db.set("test_key_2", {"data": "value_2", "count": 42})
    db.set("test_key_3", ["item1", "item2", "item3"])

    # Add nested keys
    print("Adding nested keys...")
    db.set_nested("subnet_1", "node_1", {"status": "active", "epoch": 100})
    db.set_nested("subnet_1", "node_2", {"status": "active", "epoch": 100})
    db.set_nested("subnet_2", "node_1", {"status": "inactive", "epoch": 99})

    # Add peers to named map
    print("Adding peers to named map...")
    db.nmap_set("peers", "QmPeer123", {"address": "192.168.1.1", "port": 8080})
    db.nmap_set("peers", "QmPeer456", {"address": "192.168.1.2", "port": 8081})
    db.nmap_set("peers", "QmPeer789", {"address": "192.168.1.3", "port": 8082})

    # Add heartbeats with composite keys
    print("Adding heartbeats...")
    db.nmap_set("heartbeats", "subnet_1:node_1", {"timestamp": 1234567890, "status": "ok"})
    db.nmap_set("heartbeats", "subnet_1:node_2", {"timestamp": 1234567891, "status": "ok"})
    db.nmap_set("heartbeats", "subnet_2:node_1", {"timestamp": 1234567892, "status": "degraded"})

    # Add metrics
    print("Adding metrics...")
    db.nmap_set("metrics", "cpu_usage", {"value": 45.2, "unit": "percent"})
    db.nmap_set("metrics", "memory_usage", {"value": 2048, "unit": "MB"})
    db.nmap_set("metrics", "disk_usage", {"value": 75.5, "unit": "percent"})

    print(f"\nTest database created at: {db.db_path}")
    print(f"Total keys: {len(list(db.store.keys()))}")

    # Verify data
    print("\nVerifying data...")
    print(f"Simple key: {db.get('test_key_1')}")
    print(f"Nested key: {db.get_nested('subnet_1', 'node_1')}")
    print(f"Peer: {db.nmap_get('peers', 'QmPeer123')}")
    print(f"Heartbeat: {db.nmap_get('heartbeats', 'subnet_1:node_1')}")

    db.close()
    print("\n✅ Test database populated successfully!")
    print(f"\nTo test the API, run:")
    print(f"  API_DB_PATH=/tmp/test_rocksdb python -m subnet.api.main")


if __name__ == "__main__":
    populate_test_data()
