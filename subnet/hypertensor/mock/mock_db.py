from dataclasses import asdict, is_dataclass
import json
import os
import sqlite3
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
                coldkey TEXT,
                hotkey TEXT,
                peer_id TEXT,
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
                total_node_delegate_stake_shares INTEGER,
                node_delegate_stake_balance INTEGER,
                coldkey_reputation TEXT,
                subnet_node_reputation INTEGER,
                node_slot_index INTEGER,
                consecutive_idle_epochs INTEGER,
                consecutive_included_epochs INTEGER,
                info_json TEXT
            )
            """
        )

        # Overwatch table
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS overwatch_nodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                overwatch_node_id INTEGER,
                coldkey TEXT,
                hotkey TEXT,
                peer_ids TEXT,
                reputation TEXT,
                account_overwatch_stake INTEGER,
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

        # Bootnodes table
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS bootnodes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subnet_id INTEGER,
                peer_id TEXT,
                bootnode TEXT
            )
            """
        )

        self.conn.commit()

    def reset_database(self):
        """Completely wipe the database."""
        if os.path.exists(self.db_path):
            print(f"Removing database file: {self.db_path}")
            os.remove(self.db_path)
        self._connect()
        self._create_tables()

    def insert_subnet_node(self, subnet_id: int, node_info: dict):
        print(f"Inserting node, subnet_id={subnet_id}, node_info={node_info}")
        classification_json = json.dumps(_serialize_for_json(node_info.get("classification", {})))
        coldkey_reputation_json = json.dumps(_serialize_for_json(node_info.get("coldkey_reputation", {})))

        c = self.conn.cursor()
        c.execute(
            """
            INSERT OR REPLACE INTO subnet_nodes (
                subnet_id, subnet_node_id, coldkey, hotkey, peer_id,
                bootnode_peer_id, client_peer_id, bootnode,
                identity, classification,
                delegate_reward_rate, last_delegate_reward_rate_update,
                unique_id, non_unique,
                stake_balance, total_node_delegate_stake_shares, node_delegate_stake_balance,
                coldkey_reputation, subnet_node_reputation, node_slot_index, consecutive_idle_epochs,
                consecutive_included_epochs, info_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                subnet_id,
                node_info["subnet_node_id"],
                node_info["coldkey"],
                node_info["hotkey"],
                node_info["peer_id"],
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
                int(node_info.get("total_node_delegate_stake_shares", 0)),
                int(node_info.get("node_delegate_stake_balance", 0)),
                coldkey_reputation_json,
                int(node_info.get("subnet_node_reputation", 1000000000000000000)),
                int(node_info.get("node_slot_index", node_info["subnet_node_id"])),
                int(node_info.get("consecutive_idle_epochs", 0)),
                int(node_info.get("consecutive_included_epochs", 0)),
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
        c.execute("SELECT info_json FROM subnet_nodes WHERE subnet_id = ?", (subnet_id,))
        rows = c.fetchall()

        result = []
        for row in rows:
            info = row["info_json"]
            if isinstance(info, str):
                info = json.loads(info)
            result.append(info)
        return result

    def insert_overwatch_node(self, overwatch_node_id: int, node_info: dict):
        reputation_json = json.dumps(_serialize_for_json(node_info.get("reputation", {})))
        peer_ids_json = json.dumps(_serialize_for_json(node_info.get("peer_ids", {})))

        c = self.conn.cursor()
        c.execute(
            """
            INSERT OR REPLACE INTO overwatch_nodes (
                overwatch_node_id,
                coldkey,
                hotkey,
                peer_ids,
                reputation,
                account_overwatch_stake,
                info_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                overwatch_node_id,
                node_info["coldkey"],
                node_info["hotkey"],
                peer_ids_json,
                reputation_json,
                node_info["account_overwatch_stake"],
                json.dumps(_serialize_for_json(node_info)),
            ),
        )
        self.conn.commit()

    def get_all_overwatch_nodes(self) -> list[dict]:
        c = self.conn.cursor()
        c.execute("SELECT info_json FROM overwatch_nodes")
        rows = c.fetchall()

        result = []
        for row in rows:
            info = row["info_json"]
            if isinstance(info, str):
                info = json.loads(info)
            result.append(info)
        return result

    def delete_overwatch_node(self, overwatch_node_id: int) -> bool:
        """
        Delete an overwatch node by overwatch_node_id.

        Returns True if a row was deleted, False otherwise.
        """
        c = self.conn.cursor()
        c.execute(
            "DELETE FROM overwatch_nodes WHERE overwatch_node_id = ?",
            (overwatch_node_id,),
        )
        self.conn.commit()
        return c.rowcount > 0

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

    def insert_bootnode(self, subnet_id: int, peer_id: str, bootnode: str):
        c = self.conn.cursor()
        c.execute(
            """
            INSERT OR REPLACE INTO bootnodes (
                subnet_id, peer_id, bootnode
            )
            VALUES (?, ?, ?)
            """,
            (
                subnet_id,
                peer_id,
                bootnode,
            ),
        )
        self.conn.commit()

    def get_bootnode(self, subnet_id: int, peer_id: str) -> Optional[dict]:
        c = self.conn.cursor()
        c.execute(
            "SELECT * FROM bootnodes WHERE subnet_id = ? AND peer_id = ?",
            (subnet_id, peer_id),
        )
        row = c.fetchone()
        if not row:
            return None
        return {
            "peer_id": row["peer_id"],
            "bootnode": row["bootnode"],
        }

    def get_all_bootnodes(self, subnet_id: int) -> list[dict]:
        c = self.conn.cursor()
        c.execute("SELECT * FROM bootnodes WHERE subnet_id = ?", (subnet_id,))
        rows = c.fetchall()

        result = []
        for row in rows:
            result.append(row)
        return result
