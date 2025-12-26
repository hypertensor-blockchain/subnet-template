import sqlite3
import json
import os
from dataclasses import asdict, is_dataclass
from typing import Optional

DB_FILE = "mock_hypertensor.db"


def _serialize_for_json(obj):
    """Recursively convert dataclass objects to dicts for JSON serialization."""
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    elif isinstance(obj, list):
        return [_serialize_for_json(item) for item in obj]
    elif isinstance(obj, dict):
        return {k: _serialize_for_json(v) for k, v in obj.items()}
    return obj


class MockDatabase:
    """
    Lightweight SQLite wrapper that simulates an on-chain ledger.

    Tables:
        - subnet_nodes: stores node registration info
        - consensus_data: stores per-epoch consensus proposals
    """

    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self.conn = None
        self._connect()
        self._create_tables()

    def _connect(self):
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def _create_tables(self):
        c = self.conn.cursor()

        # Nodes table
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS subnet_nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subnet_id INTEGER,
                subnet_node_id INTEGER UNIQUE,
                peer_id TEXT,
                coldkey TEXT,
                hotkey TEXT,
                bootnode_peer_id TEXT,
                client_peer_id TEXT,
                bootnode TEXT,
                identity TEXT,
                classification TEXT,
                delegate_reward_rate INTEGER,
                last_delegate_reward_rate_update INTEGER,
                unique_id TEXT,
                non_unique TEXT,
                stake_balance INTEGER,
                node_delegate_stake_balance INTEGER,
                penalties INTEGER,
                reputation INTEGER,
                info_json TEXT
            )
            """
        )

        # Consensus data table
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS consensus_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subnet_id INTEGER,
                epoch INTEGER,
                validator_id INTEGER,
                validator_epoch_progress INTEGER,
                attests_json TEXT,
                subnet_nodes_json TEXT,
                prioritize_queue_node_id INTEGER,
                remove_queue_node_id INTEGER,
                data_json TEXT,
                args_json TEXT
            )
            """
        )
        self.conn.commit()

    def reset_database(self):
        """Completely wipe the database."""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self._connect()
        self._create_tables()

    def insert_subnet_node(self, subnet_id: int, node_info: dict):
        classification_json = json.dumps(
            _serialize_for_json(node_info.get("classification", {}))
        )

        c = self.conn.cursor()
        c.execute(
            """
            INSERT OR REPLACE INTO subnet_nodes (
                subnet_id, subnet_node_id, peer_id,
                coldkey, hotkey, bootnode_peer_id,
                client_peer_id, bootnode, 
                identity, classification,
                delegate_reward_rate, last_delegate_reward_rate_update,
                unique_id, non_unique,
                stake_balance, node_delegate_stake_balance,
                penalties, reputation,
                info_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                subnet_id,
                node_info["subnet_node_id"],
                node_info["peer_id"],
                node_info["coldkey"],
                node_info["hotkey"],
                node_info["bootnode_peer_id"],
                node_info["client_peer_id"],
                node_info["bootnode"],
                node_info["identity"],
                classification_json,
                node_info["delegate_reward_rate"],
                node_info["last_delegate_reward_rate_update"],
                node_info["unique"],
                node_info["non_unique"],
                int(node_info.get("stake_balance", 0)),
                int(node_info.get("node_delegate_stake_balance", 0)),
                int(node_info.get("penalties", 0)),
                int(node_info.get("reputation", 0)),
                json.dumps(_serialize_for_json(node_info)),
            ),
        )
        self.conn.commit()

    def delete_subnet_node(self, subnet_id: int, subnet_node_id: int) -> bool:
        """
        Delete a subnet node by subnet_id and subnet_node_id.

        Returns True if a row was deleted, False otherwise.
        """
        c = self.conn.cursor()
        c.execute(
            "DELETE FROM subnet_nodes WHERE subnet_id = ? AND subnet_node_id = ?",
            (subnet_id, subnet_node_id),
        )
        self.conn.commit()
        return c.rowcount > 0

    def get_all_subnet_nodes(self, subnet_id: int) -> list[dict]:
        c = self.conn.cursor()
        c.execute(
            "SELECT info_json FROM subnet_nodes WHERE subnet_id = ?", (subnet_id,)
        )
        rows = c.fetchall()

        result = []
        for row in rows:
            info = row["info_json"]
            if isinstance(info, str):
                info = json.loads(info)
            result.append(info)
        return result

    def insert_consensus_data(self, subnet_id: int, epoch: int, data: dict):
        c = self.conn.cursor()
        c.execute(
            """
            INSERT OR REPLACE INTO consensus_data (
                subnet_id, epoch, validator_id,
                validator_epoch_progress,
                attests_json, subnet_nodes_json,
                prioritize_queue_node_id, remove_queue_node_id,
                data_json, args_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                subnet_id,
                epoch,
                data["validator_id"],
                data.get("validator_epoch_progress", 0),
                json.dumps(_serialize_for_json(data.get("attests", []))),
                json.dumps(_serialize_for_json(data.get("subnet_nodes", []))),
                data.get("prioritize_queue_node_id"),
                data.get("remove_queue_node_id"),
                json.dumps(_serialize_for_json(data.get("data", []))),
                json.dumps(_serialize_for_json(data.get("args"))),
            ),
        )
        self.conn.commit()

    def get_consensus_data(self, subnet_id: int, epoch: int) -> Optional[dict]:
        c = self.conn.cursor()
        c.execute(
            "SELECT * FROM consensus_data WHERE subnet_id = ? AND epoch = ?",
            (subnet_id, epoch),
        )
        row = c.fetchone()
        if not row:
            return None
        return {
            "validator_id": row["validator_id"],
            "validator_epoch_progress": row["validator_epoch_progress"],
            "attests": json.loads(row["attests_json"]),
            "subnet_nodes": json.loads(row["subnet_nodes_json"]),
            "prioritize_queue_node_id": row["prioritize_queue_node_id"],
            "remove_queue_node_id": row["remove_queue_node_id"],
            "data": json.loads(row["data_json"]),
            "args": json.loads(row["args_json"]) if row["args_json"] else None,
        }
