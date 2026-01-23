from typing import List, Optional

from libp2p.peer.id import ID as PeerID
from subnet.hypertensor.chain_data import (
    ConsensusData,
    SubnetNodeConsensusData,
    SubnetNodeInfo,
)
from subnet.hypertensor.chain_functions import Hypertensor


def compare_consensus_data(
    my_data: List[SubnetNodeConsensusData],
    validator_data: List[SubnetNodeConsensusData],
) -> float:
    validator_data_set = set(frozenset(validator_data))
    my_data_set = set(frozenset(my_data))

    intersection = my_data_set & validator_data_set
    union = my_data_set | validator_data_set

    if not union:
        return 100.0

    # Accuracy as a percentage of overlap
    accuracy = float(len(intersection) / len(union))
    return accuracy


def get_attestation_ratio(consensus_data: ConsensusData):
    return len(consensus_data.attests) / len(consensus_data.subnet_nodes)


def did_node_attest(subnet_node_id: int, consensus_data: ConsensusData) -> bool:
    for attest in consensus_data.attests:
        if attest.attestor_id == subnet_node_id:
            return True
    return False


def is_validator_or_attestor(hypertensor: Hypertensor, subnet_id: int, subnet_node_id: int) -> bool:
    validators_and_attestors = hypertensor.get_validators_and_attestors_formatted(subnet_id)
    print("is_validator_or_attestor validators_and_attestors: ", validators_and_attestors)
    if validators_and_attestors is None:
        return False

    for item in validators_and_attestors:
        if item.subnet_node_id == subnet_node_id:
            return True
    return False


def get_peers_node_id(peer_id: PeerID, subnet_nodes_info: List[SubnetNodeInfo]) -> Optional[int]:
    """Return the subnet_node_id for the given peer_id, or None if not found."""
    return next(
        (node.subnet_node_id for node in subnet_nodes_info if peer_id.__eq__(node.peer_id)),
        None,  # default value if not found
    )
