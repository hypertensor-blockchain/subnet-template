"""Assembly object for one DAG namespace/topic runtime."""

from __future__ import annotations

from collections.abc import Sequence

from libp2p.peer.id import ID

from subnet.merkle_dag import (
    CanonicalJSONSerializer,
    DagSyncMessageCodec,
    DagValidator,
    Libp2pSignatureVerifier,
    MerkleDag,
    MerkleDagSyncCoordinator,
    PayloadSchemaRegistry,
    RocksDBDagStorage,
    SHA256Hasher,
)
from subnet.merkle_dag.interfaces import DagStorage, GossipPublisher, PayloadSchema, PeerRequestClient
from subnet.utils.db.database import RocksDB
from subnet.utils.pubsub.topics import DEFAULT_DAG_TOPIC


class MerkleDagRuntime:
    """Own the assembled DAG primitives and coordinator for one namespace/topic."""

    def __init__(
        self,
        db: RocksDB,
        payload_schemas: Sequence[PayloadSchema],
        *,
        local_peer_id: ID,
        namespace: str = "default",
        dag_topic: str = DEFAULT_DAG_TOPIC,
        storage: DagStorage | None = None,
        request_client: PeerRequestClient | None = None,
        gossip_publisher: GossipPublisher | None = None,
        max_fetch_batch: int = 32,
        max_ancestor_depth: int = 64,
    ):
        self.db = db
        self.namespace = namespace
        self.dag_topic = dag_topic
        self.local_peer_id = local_peer_id.to_string()

        self.serializer = CanonicalJSONSerializer()
        self.hasher = SHA256Hasher()
        self.codec = DagSyncMessageCodec(self.serializer)
        self.schema_registry = PayloadSchemaRegistry(list(payload_schemas))
        self.signature_verifier = Libp2pSignatureVerifier()
        self.storage = storage or RocksDBDagStorage(db, self.serializer, namespace=namespace)
        self.validator = DagValidator(
            serializer=self.serializer,
            hasher=self.hasher,
            schema_registry=self.schema_registry,
            signature_verifier=self.signature_verifier,
        )
        self.dag = MerkleDag(
            namespace=namespace,
            storage=self.storage,
            validator=self.validator,
            schema_registry=self.schema_registry,
            serializer=self.serializer,
        )
        self.coordinator = MerkleDagSyncCoordinator(
            dag=self.dag,
            local_peer_id=self.local_peer_id,
            topic=dag_topic,
            codec=self.codec,
            gossip_publisher=gossip_publisher,
            request_client=request_client,
            max_fetch_batch=max_fetch_batch,
            max_ancestor_depth=max_ancestor_depth,
        )
