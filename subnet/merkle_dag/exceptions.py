"""Custom exceptions used by the Merkle DAG subsystem."""


class MerkleDagError(Exception):
    """Base exception for Merkle DAG failures."""


class SerializationError(MerkleDagError):
    """Raised when canonical serialization fails."""


class SchemaNotFoundError(MerkleDagError):
    """Raised when a payload schema is not registered locally."""


class PayloadValidationError(MerkleDagError):
    """Raised when a payload does not satisfy its schema."""


class HashMismatchError(MerkleDagError):
    """Raised when a computed content hash differs from the supplied hash."""


class SignatureVerificationError(MerkleDagError):
    """Raised when a node signature cannot be verified."""


class SourcePeerMismatchError(MerkleDagError):
    """Raised when the transport sender does not match the node's signed identity."""


class ParentValidationError(MerkleDagError):
    """Raised when parent linkage is malformed or invalid."""


class TimestampValidationError(MerkleDagError):
    """Raised when a remote node timestamp is implausible relative to local time."""


class UnknownNodeError(MerkleDagError):
    """Raised when a requested node cannot be found."""


class MessageDecodingError(MerkleDagError):
    """Raised when a wire message cannot be decoded."""
