from typing import Callable

from libp2p.peer.id import ID
from libp2p.records.pubkey import PublicKeyValidator, unmarshal_public_key
from libp2p.records.utils import InvalidRecordType, split_key
from libp2p.records.validator import Validator
import multihash

from subnet.hypertensor.chain_functions import EpochData


class PredicateValidator(Validator):
    def __init__(
        self,
        pkv: PublicKeyValidator,
        record_predicate: Callable[[str, bytes], bool],
    ):
        super().__init__()
        self.pkv = pkv
        self.record_predicate = record_predicate

    @classmethod
    def from_predicate_class(cls, predicate_cls: type, *args, **kwargs) -> "PredicateValidator":
        """
        Example:
            PredicateValidator.from_predicate_class(
                CommitReveal, Hypertensor(), subnet_id
            )

        """
        predicate = predicate_cls(*args, **kwargs)
        return cls(record_predicate=predicate)

    def validate(self, key: str, value: bytes) -> None:
        try:
            self.pkv.validate(key, value)
        except InvalidRecordType:
            raise

        if not self.record_predicate(key, value):
            raise InvalidRecordType("Predicate failed")

    def select(self, key: str, values: list[bytes]) -> int:
        return 0


# Example predicate validator


class ExamplePredicateValidator:
    """
    Example predicate validator for subnet records.

    params:
        key: The namespace key to validate
    """

    def __init__(self, key: str):
        self.key = key

    def __call__(self, key: str, value: bytes) -> bool:
        ns, key = split_key(key)
        if ns != self.key:
            raise InvalidRecordType(f"namespace not '{self.key}'")

        keyhash = bytes.fromhex(key)
        try:
            _ = multihash.decode(keyhash)
        except Exception:
            raise InvalidRecordType("key did not contain valid multihash")

        try:
            pubkey = unmarshal_public_key(value)
        except Exception:
            raise InvalidRecordType("Unable to unmarshal public key")

        try:
            peer_id = ID.from_pubkey(pubkey)
        except Exception:
            raise InvalidRecordType("Could not derive peer ID from public key")

        if peer_id.to_bytes() != keyhash:
            raise InvalidRecordType("public key does not match storage key")

        return True


class HypertensorExamplePredicateValidator:
    """
    Example predicate validator for subnet records.

    params:
        key: The namespace key to validate
        hypertensor: The hypertensor substrate RPC connection
        subnet_id: The subnet ID to validate against
        min_elapsed: The minimum elapsed time of the subnets epoch referenced as a percentage
        max_elapsed: The maximum elapsed time of the subnets epoch referenced as a percentage
    """

    def __init__(
        self,
        key: str,
        hypertensor,
        subnet_id: int,
        min_elapsed: float = 0.0,
        max_elapsed: float = 1.0,
    ):
        self.key = key
        self.hypertensor = hypertensor
        self.subnet_id = subnet_id
        self.min_elapsed = min_elapsed
        self.max_elapsed = max_elapsed
        self.slot: int | None = None
        self._ensure_slot()

    def _ensure_slot(self):
        if self.slot is None:
            subnet_info = self.hypertensor.get_formatted_subnet_info(self.subnet_id)
            self.slot = subnet_info.slot_index
        return self.slot

    def epoch_data(self) -> EpochData:
        """Returns the current epoch data"""
        return self.hypertensor.get_subnet_epoch_data(self._ensure_slot())

    def __call__(self, key: str, value: bytes) -> bool:
        epoch_data = self.epoch_data()
        epoch = epoch_data.epoch

        ns, key = split_key(key)
        if ns != f"{self.key}_{epoch}":
            raise InvalidRecordType(f"namespace not '{self.key}_{epoch}'")

        keyhash = bytes.fromhex(key)
        try:
            _ = multihash.decode(keyhash)
        except Exception:
            raise InvalidRecordType("key did not contain valid multihash")

        try:
            pubkey = unmarshal_public_key(value)
        except Exception:
            raise InvalidRecordType("Unable to unmarshal public key")

        try:
            peer_id = ID.from_pubkey(pubkey)
        except Exception:
            raise InvalidRecordType("Could not derive peer ID from public key")

        if peer_id.to_bytes() != keyhash:
            raise InvalidRecordType("public key does not match storage key")

        percent_complete = epoch_data.percent_complete

        if percent_complete <= self.min_elapsed or percent_complete > self.max_elapsed:
            raise InvalidRecordType("epoch not in range")

        return True
