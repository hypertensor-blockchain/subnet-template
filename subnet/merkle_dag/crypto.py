"""Pluggable hashing and signature helpers for Merkle DAG nodes."""

from __future__ import annotations

import hashlib

from libp2p.crypto.keys import KeyPair
from libp2p.records.pubkey import unmarshal_public_key


class SHA256Hasher:
    """SHA-256 hash provider that prefixes digests with the algorithm name."""

    algorithm = "sha256"

    def digest(self, payload: bytes) -> str:
        """Compute a stable SHA-256 digest."""
        return f"{self.algorithm}:{hashlib.sha256(payload).hexdigest()}"


class Libp2pKeyPairSigner:
    """Signs payloads with an existing libp2p key pair."""

    def __init__(self, key_pair: KeyPair):
        self._key_pair = key_pair

    def public_key_bytes(self) -> bytes:
        """Return the serialized public key bytes."""
        return self._key_pair.public_key.serialize()

    def sign(self, payload: bytes) -> bytes:
        """Sign the supplied bytes."""
        return self._key_pair.private_key.sign(payload)


class Libp2pSignatureVerifier:
    """Verifies signatures against serialized libp2p public keys."""

    def verify(self, payload: bytes, signature: bytes, public_key: bytes) -> bool:
        """Verify a detached signature."""
        key = unmarshal_public_key(public_key)
        return key.verify(payload, signature)
