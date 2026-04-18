from __future__ import annotations

from dataclasses import replace

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID

from subnet.merkle_dag import (
    CanonicalJSONSerializer,
    DagValidator,
    InMemoryDagStorage,
    Libp2pKeyPairSigner,
    Libp2pSignatureVerifier,
    MerkleDag,
    PayloadSchemaRegistry,
    SHA256Hasher,
)
from subnet.merkle_dag.exceptions import (
    ParentValidationError,
    PayloadValidationError,
    SignatureVerificationError,
    SourcePeerMismatchError,
    TimestampValidationError,
)
from subnet.merkle_dag.payloads import MappingPayloadSchema


class CounterPayloadSchema(MappingPayloadSchema):
    def __init__(self):
        super().__init__("counter")

    def validate_payload(self, payload):
        super().validate_payload(payload)
        if not isinstance(payload.get("value"), int):
            raise PayloadValidationError("counter payload requires an integer 'value'")

    def materialize(self, node, parent_states):
        return sum(parent_states) + int(node.body.payload["value"])


class PeerOwnedStatusSchema(MappingPayloadSchema):
    def __init__(self):
        super().__init__("peer-status")

    def validate_payload(self, payload):
        super().validate_payload(payload)
        if not isinstance(payload.get("peer_id"), str) or not payload["peer_id"]:
            raise PayloadValidationError("peer-status payload requires a non-empty 'peer_id'")
        if not isinstance(payload.get("status"), str) or not payload["status"]:
            raise PayloadValidationError("peer-status payload requires a non-empty 'status'")

    def validate_signer_peer(self, node, signer_peer_id: str) -> None:
        payload_peer_id = node.body.payload.get("peer_id")
        if payload_peer_id != signer_peer_id:
            raise PayloadValidationError(
                f"peer-status payload peer_id {payload_peer_id!r} does not match signer peer {signer_peer_id!r}"
            )

    def validate_parent_links(self, node, parents) -> None:
        payload_peer_id = node.body.payload["peer_id"]
        for parent in parents:
            parent_payload_peer_id = parent.body.payload.get("peer_id")
            if parent_payload_peer_id != payload_peer_id:
                raise PayloadValidationError("peer-status parents must belong to the same peer_id")


def _build_dag(
    namespace: str = "test-dag",
    schemas: list[MappingPayloadSchema] | None = None,
    *,
    now_ms=None,
) -> MerkleDag:
    serializer = CanonicalJSONSerializer()
    schema_registry = PayloadSchemaRegistry(schemas or [CounterPayloadSchema()])
    validator = DagValidator(
        serializer=serializer,
        hasher=SHA256Hasher(),
        schema_registry=schema_registry,
        signature_verifier=Libp2pSignatureVerifier(),
        now_ms=now_ms,
    )
    return MerkleDag(
        namespace=namespace,
        storage=InMemoryDagStorage(),
        validator=validator,
        schema_registry=schema_registry,
        serializer=serializer,
    )


def _signer(seed_byte: int) -> Libp2pKeyPairSigner:
    return Libp2pKeyPairSigner(create_new_key_pair(bytes([seed_byte]) * 32))


def _signer_and_peer_id(seed_byte: int) -> tuple[Libp2pKeyPairSigner, str]:
    key_pair = create_new_key_pair(bytes([seed_byte]) * 32)
    return Libp2pKeyPairSigner(key_pair), ID.from_pubkey(key_pair.public_key).to_string()


@pytest.mark.asyncio
async def test_deterministic_hashing():
    dag = _build_dag()
    signer = _signer(1)

    node_a = await dag.create_node(
        schema_id="counter",
        payload={"value": 5, "meta": {"b": 2, "a": 1}},
        parent_ids=(),
        signer=signer,
        author="peer-a",
        created_at_ms=1000,
    )
    node_b = await dag.create_node(
        schema_id="counter",
        payload={"meta": {"a": 1, "b": 2}, "value": 5},
        parent_ids=(),
        signer=signer,
        author="peer-a",
        created_at_ms=1000,
    )

    assert node_a.header.node_id == node_b.header.node_id
    assert node_a.header.body_hash == node_b.header.body_hash


@pytest.mark.asyncio
async def test_signature_verification():
    dag = _build_dag()
    signer = _signer(2)

    node = await dag.create_node(
        schema_id="counter",
        payload={"value": 1},
        parent_ids=(),
        signer=signer,
        author="peer-b",
        created_at_ms=1001,
    )
    result = await dag.add_node(node)

    assert result.status.value == "accepted"
    assert await dag.get_node(node.header.node_id) is not None


@pytest.mark.asyncio
async def test_out_of_order_arrival():
    dag = _build_dag()
    signer = _signer(3)

    parent = await dag.create_node(
        schema_id="counter",
        payload={"value": 1},
        parent_ids=(),
        signer=signer,
        author="peer-c",
        created_at_ms=1002,
    )
    child = await dag.create_node(
        schema_id="counter",
        payload={"value": 2},
        parent_ids=(parent.header.node_id,),
        signer=signer,
        author="peer-c",
        created_at_ms=1003,
    )

    child_result = await dag.add_node(child)
    assert child_result.status.value == "orphan"
    assert child_result.missing_parents == (parent.header.node_id,)
    assert await dag.get_heads() == ()

    parent_result = await dag.add_node(parent)
    assert parent_result.status.value == "accepted"
    assert child.header.node_id in parent_result.resolved_nodes
    assert await dag.get_heads() == (child.header.node_id,)
    assert await dag.storage.get_orphan(child.header.node_id) is None


@pytest.mark.asyncio
async def test_orphan_resolution_after_parent_fetch():
    dag = _build_dag()
    signer = _signer(4)

    parent = await dag.create_node(
        schema_id="counter",
        payload={"value": 10},
        parent_ids=(),
        signer=signer,
        author="peer-d",
        created_at_ms=1004,
    )
    child = await dag.create_node(
        schema_id="counter",
        payload={"value": 11},
        parent_ids=(parent.header.node_id,),
        signer=signer,
        author="peer-d",
        created_at_ms=1005,
    )

    await dag.add_node(child)
    fetch_result = await dag.add_node(parent)

    assert fetch_result.resolved_nodes == (parent.header.node_id, child.header.node_id)
    assert await dag.get_heads() == (child.header.node_id,)


@pytest.mark.asyncio
async def test_multiple_heads():
    dag = _build_dag()
    signer = _signer(5)

    root = await dag.create_node(
        schema_id="counter",
        payload={"value": 1},
        parent_ids=(),
        signer=signer,
        author="peer-e",
        created_at_ms=1006,
    )
    left = await dag.create_node(
        schema_id="counter",
        payload={"value": 2},
        parent_ids=(root.header.node_id,),
        signer=signer,
        author="peer-e",
        created_at_ms=1007,
    )
    right = await dag.create_node(
        schema_id="counter",
        payload={"value": 3},
        parent_ids=(root.header.node_id,),
        signer=signer,
        author="peer-e",
        created_at_ms=1008,
    )

    await dag.add_node(root)
    await dag.add_node(left)
    await dag.add_node(right)

    assert set(await dag.get_heads()) == {left.header.node_id, right.header.node_id}


@pytest.mark.asyncio
async def test_invalid_parent_rejection():
    dag = _build_dag()
    signer = _signer(6)

    node = await dag.create_node(
        schema_id="counter",
        payload={"value": 1},
        parent_ids=(),
        signer=signer,
        author="peer-f",
        created_at_ms=1009,
    )
    malformed = replace(node, header=replace(node.header, parent_ids=(node.header.node_id,)))

    with pytest.raises(ParentValidationError):
        await dag.add_node(malformed)


@pytest.mark.asyncio
async def test_invalid_signature_rejection():
    dag = _build_dag()
    signer = _signer(7)

    node = await dag.create_node(
        schema_id="counter",
        payload={"value": 1},
        parent_ids=(),
        signer=signer,
        author="peer-g",
        created_at_ms=1010,
    )
    bad_signature = "00" * (len(node.header.signature) // 2)
    tampered = replace(node, header=replace(node.header, signature=bad_signature))

    with pytest.raises(SignatureVerificationError):
        await dag.add_node(tampered)


@pytest.mark.asyncio
async def test_validator_can_compare_transport_sender_to_signed_identity():
    dag = _build_dag()
    signer, signer_peer_id = _signer_and_peer_id(8)
    _, different_peer_id = _signer_and_peer_id(9)

    node = await dag.create_node(
        schema_id="counter",
        payload={"value": 1},
        parent_ids=(),
        signer=signer,
        author=signer_peer_id,
        created_at_ms=1011,
    )

    with pytest.raises(SourcePeerMismatchError):
        dag.validator.validate_header_source_peer(node.header, different_peer_id)


@pytest.mark.asyncio
async def test_remote_node_rejects_created_at_far_in_future():
    dag = _build_dag(now_ms=lambda: 1_000)
    signer = _signer(12)

    node = await dag.create_node(
        schema_id="counter",
        payload={"value": 1},
        parent_ids=(),
        signer=signer,
        author="peer-h",
        created_at_ms=61_001,
    )

    with pytest.raises(TimestampValidationError):
        await dag.add_node(node, validate_remote_timestamp=True)


@pytest.mark.asyncio
async def test_local_node_add_can_use_fixed_created_at_without_remote_timestamp_check():
    dag = _build_dag(now_ms=lambda: 1_000)
    signer = _signer(13)

    node = await dag.create_node(
        schema_id="counter",
        payload={"value": 1},
        parent_ids=(),
        signer=signer,
        author="peer-i",
        created_at_ms=61_001,
    )

    result = await dag.add_node(node)

    assert result.status.value == "accepted"


@pytest.mark.asyncio
async def test_schema_can_enforce_payload_peer_ownership():
    dag = _build_dag(schemas=[CounterPayloadSchema(), PeerOwnedStatusSchema()])
    signer, signer_peer_id = _signer_and_peer_id(10)
    _, other_peer_id = _signer_and_peer_id(11)

    node = await dag.create_node(
        schema_id="peer-status",
        payload={"peer_id": other_peer_id, "status": "online"},
        parent_ids=(),
        signer=signer,
        author=signer_peer_id,
        created_at_ms=1012,
    )

    with pytest.raises(PayloadValidationError):
        await dag.add_node(node)
