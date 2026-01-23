import os
import shutil
from typing import Any

from rocksdict import Rdict


class RocksDB:
    """
    RocksDB wrapper using rocksdict with support for:
    - Simple key:value storage
    - Nested key storage (k1:k2:value)
    - Named map storage (nmap:key:value)
    """

    SEPARATOR = ":"

    def __init__(self, base_path: str | None = None, subnet_id: int = 1):
        assert base_path is not None, "Path must be specified"
        self.base_path = base_path
        self.subnet_id = subnet_id
        self.db_path = f"{base_path}_store"
        self.store = Rdict(self.db_path)

    # =========================================================================
    # Simple key:value storage
    # =========================================================================

    def set(self, key: str, value: Any) -> None:
        """Store a value by its key."""
        self.store[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value by its key."""
        try:
            return self.store[key]
        except KeyError:
            return default

    def delete(self, key: str) -> bool:
        """Delete a key. Returns True if key existed, False otherwise."""
        try:
            del self.store[key]
            return True
        except KeyError:
            return False

    def exists(self, key: str) -> bool:
        """Check if a key exists."""
        return key in self.store

    # =========================================================================
    # Nested key storage (k1:k2:value)
    # =========================================================================

    def _make_nested_key(self, k1: str, k2: str) -> str:
        """Create a composite key from k1 and k2."""
        return f"{k1}{self.SEPARATOR}{k2}"

    def set_nested(self, k1: str, k2: str, value: Any) -> None:
        """Store a value under nested keys k1:k2."""
        composite_key = self._make_nested_key(k1, k2)
        self.store[composite_key] = value

    def get_nested(self, k1: str, k2: str, default: Any = None) -> Any:
        """Retrieve a value under nested keys k1:k2."""
        composite_key = self._make_nested_key(k1, k2)
        try:
            return self.store[composite_key]
        except KeyError:
            return default

    def delete_nested(self, k1: str, k2: str) -> bool:
        """Delete a nested key. Returns True if key existed."""
        composite_key = self._make_nested_key(k1, k2)
        try:
            del self.store[composite_key]
            return True
        except KeyError:
            return False

    def get_all_under_key(self, k1: str) -> dict[str, Any]:
        """
        Get all subkeys and values under a given key k1.
        Returns a dict of {k2: value} for all k1:k2 entries.
        """
        prefix = f"{k1}{self.SEPARATOR}"
        results = {}
        for key in self.store.keys():
            if isinstance(key, str) and key.startswith(prefix):
                # Extract k2 from the composite key
                k2 = key[len(prefix) :]
                # Only include if k2 doesn't contain another separator (direct children only)
                if self.SEPARATOR not in k2:
                    results[k2] = self.store[key]
        return results

    def get_all_under_key_recursive(self, k1: str) -> dict[str, Any]:
        """
        Get all subkeys and values under a given key k1 (including nested).
        Returns a dict of {remaining_key: value} for all entries starting with k1:.
        """
        prefix = f"{k1}{self.SEPARATOR}"
        results = {}
        for key in self.store.keys():
            if isinstance(key, str) and key.startswith(prefix):
                remaining_key = key[len(prefix) :]
                results[remaining_key] = self.store[key]
        return results

    # =========================================================================
    # Named map storage (nmap:key:value)
    # =========================================================================

    def _make_nmap_key(self, nmap: str, key: str) -> str:
        """Create a namespaced key for named maps."""
        return f"nmap{self.SEPARATOR}{nmap}{self.SEPARATOR}{key}"

    def nmap_set(self, nmap: str, key: str, value: Any) -> None:
        """
        Store a value in a named map.

        Named maps allow you to organize data into logical groups/namespaces.
        Internally stored as 'nmap:<nmap>:<key>' -> value.

        Args:
            nmap: The name of the map (namespace/category).
            key: The key within the named map.
            value: The value to store.

        Example:
            # Simple 2-key usage: nmap='users', key='alice'
            db.nmap_set('users', 'alice', {'age': 30, 'role': 'admin'})
            db.nmap_set('users', 'bob', {'age': 25, 'role': 'user'})

            # 3+ keys using composite key with separator:
            # nmap='heartbeats', key='subnet_1:node_5' (3 keys total)
            db.nmap_set('heartbeats', 'subnet_1:node_5', {'epoch': 100, 'status': 'ok'})
            db.nmap_set('heartbeats', 'subnet_1:node_6', {'epoch': 100, 'status': 'ok'})
            db.nmap_set('heartbeats', 'subnet_2:node_1', {'epoch': 100, 'status': 'ok'})

            # 4 keys using composite key:
            # nmap='metrics', key='subnet_1:node_5:epoch_100' (4 keys total)
            db.nmap_set('metrics', 'subnet_1:node_5:epoch_100', {'latency': 50})

            # Retrieve using the same composite key pattern:
            db.nmap_get('heartbeats', 'subnet_1:node_5')
            db.nmap_get('metrics', 'subnet_1:node_5:epoch_100')

        """
        composite_key = self._make_nmap_key(nmap, key)
        self.store[composite_key] = value

    def nmap_get(self, nmap: str, key: str, default: Any = None) -> Any:
        """
        Retrieve a value from a named map.

        Args:
            nmap: The name of the map (namespace/category).
            key: The key within the named map.
            default: Value to return if key doesn't exist (default: None).

        Returns:
            The stored value, or default if not found.

        Example:
            # Simple 2-key usage: nmap='users', key='alice'
            user = db.nmap_get('users', 'alice')
            # Returns: {'age': 30, 'role': 'admin'}

            # 3+ keys using composite key with separator:
            # nmap='heartbeats', key='subnet_1:node_5' (3 keys total)
            user = db.nmap_get('heartbeats', 'subnet_1:node_5')
            # Returns: {'epoch': 100, 'status': 'ok'}

            # 4 keys using composite key:
            # nmap='metrics', key='subnet_1:node_5:epoch_100' (4 keys total)
            user = db.nmap_get('metrics', 'subnet_1:node_5:epoch_100')
            # Returns: {'latency': 50}

            missing = db.nmap_get('users', 'charlie', default={})
            # Returns: {} (the default)

        """
        composite_key = self._make_nmap_key(nmap, key)
        try:
            return self.store[composite_key]
        except KeyError:
            return default

    def nmap_delete(self, nmap: str, key: str) -> bool:
        """
        Delete a key from a named map.

        Args:
            nmap: The name of the map (namespace/category).
            key: The key to delete within the named map.

        Returns:
            True if the key existed and was deleted, False otherwise.

        Example:
            deleted = db.nmap_delete('users', 'alice')
            # Returns: True (if alice existed)

        """
        composite_key = self._make_nmap_key(nmap, key)
        try:
            del self.store[composite_key]
            return True
        except KeyError:
            return False

    def nmap_get_all(self, nmap: str) -> dict[str, Any]:
        """
        Get all key-value pairs in a named map.

        Args:
            nmap: The name of the map (namespace/category).

        Returns:
            A dict of {key: value} for all entries in the named map.

        Example:
            db.nmap_set('users', 'alice', {'age': 30})
            db.nmap_set('users', 'bob', {'age': 25})

            all_users = db.nmap_get_all('users')
            # Returns: {'alice': {'age': 30}, 'bob': {'age': 25}}

        """
        prefix = f"nmap{self.SEPARATOR}{nmap}{self.SEPARATOR}"
        results = {}
        for key in self.store.keys():
            if isinstance(key, str) and key.startswith(prefix):
                actual_key = key[len(prefix) :]
                results[actual_key] = self.store[key]
        return results

    def nmap_exists(self, nmap: str, key: str) -> bool:
        """
        Check if a key exists in a named map.

        Args:
            nmap: The name of the map (namespace/category).
            key: The key to check within the named map.

        Returns:
            True if the key exists, False otherwise.

        Example:
            if db.nmap_exists('users', 'alice'):
                print('Alice is in the users map')

        """
        composite_key = self._make_nmap_key(nmap, key)
        return composite_key in self.store

    def nmap_clear(self, nmap: str) -> int:
        """
        Clear all entries in a named map.

        Args:
            nmap: The name of the map (namespace/category) to clear.

        Returns:
            The count of deleted entries.

        Example:
            db.nmap_set('temp', 'a', 1)
            db.nmap_set('temp', 'b', 2)

            deleted_count = db.nmap_clear('temp')
            # Returns: 2 (both entries deleted)

        """
        prefix = f"nmap{self.SEPARATOR}{nmap}{self.SEPARATOR}"
        keys_to_delete = [key for key in self.store.keys() if isinstance(key, str) and key.startswith(prefix)]
        for key in keys_to_delete:
            del self.store[key]
        return len(keys_to_delete)

    # =========================================================================
    # Database management
    # =========================================================================

    def close(self) -> None:
        """Close the database connection."""
        self.store.close()

    def destroy(self) -> None:
        """
        Close and destroy the database, removing all data.
        After calling this, the database instance should not be used.
        """
        self.store.close()
        if os.path.exists(self.db_path):
            shutil.rmtree(self.db_path)

    def __enter__(self) -> "RocksDB":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - closes the database."""
        self.close()
