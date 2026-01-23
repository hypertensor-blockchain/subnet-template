from dataclasses import dataclass
import logging
from typing import Any

import pytest
import scalecodec
from scalecodec.base import RuntimeConfiguration, ScaleBytes
from scalecodec.type_registry import load_type_registry_preset
from substrateinterface.exceptions import SubstrateRequestException

from subnet.hypertensor.chain_data import ConsensusData, custom_rpc_type_registry, get_runtime_config
from subnet.hypertensor.chain_functions import Hypertensor, KeypairFrom, SubnetNodeClass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


LOCAL_RPC = "ws://127.0.0.1:9944"

# pytest tests/substrate/test_rpc.py -rP

hypertensor = Hypertensor(
    LOCAL_RPC,
    "0x5fb92d6e98884f76de468fa3f6278f8807c48bebc13595d45af5bdc4da702133",
    keypair_from=KeypairFrom.PRIVATE_KEY,
    runtime_config=get_runtime_config(),
)
# hypertensor = Hypertensor(LOCAL_RPC, "0x5fb92d6e98884f76de468fa3f6278f8807c48bebc13595d45af5bdc4da702133", keypair_from=KeypairFrom.PRIVATE_KEY)

# pytest tests/substrate/test_rpc.py::test_get_subnet_info -rP


# def test_get_subnet_info():
#     rpc_runtime_config = RuntimeConfiguration()
#     rpc_runtime_config.update_type_registry(load_type_registry_preset("legacy"))
#     rpc_runtime_config.update_type_registry(custom_rpc_type_registry)

#     with hypertensor.interface as _interface:
#         result = _interface.rpc_request(method="network_getSubnetInfo", params=[1])
#         scale_obj = rpc_runtime_config.create_scale_object("Option<SubnetInfo>")
#         type_info = scale_obj.generate_type_decomposition()
#         print("type_info", type_info)

#         if "result" in result and result["result"]:
#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object("Option<SubnetInfo>")

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode(ScaleBytes(bytes(result["result"])))
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))

#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object(
#                     "Option<SubnetInfo>", data=ScaleBytes(bytes(result["result"]))
#                 )

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode()
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))


# # pytest tests/substrate/test_rpc.py::test_get_subnet_info_formatted -rP


# def test_get_subnet_info_formatted():
#     subnet_info = hypertensor.get_formatted_subnet_info(1)
#     print("subnet_info", subnet_info)


# # pytest tests/substrate/test_rpc.py::test_get_all_subnet_info -rP


# def test_get_all_subnet_info():
#     rpc_runtime_config = RuntimeConfiguration()
#     rpc_runtime_config.update_type_registry(load_type_registry_preset("legacy"))
#     rpc_runtime_config.update_type_registry(custom_rpc_type_registry)

#     with hypertensor.interface as _interface:
#         result = _interface.rpc_request(method="network_getAllSubnetsInfo", params=[])

#         if "result" in result and result["result"]:
#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object("Vec<SubnetInfo>")

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode(ScaleBytes(bytes(result["result"])))
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))

#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object(
#                     "Vec<SubnetInfo>", data=ScaleBytes(bytes(result["result"]))
#                 )

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode()
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))


# # pytest tests/substrate/test_rpc.py::test_get_all_subnet_info_formatted -rP


# def test_get_all_subnet_info_formatted():
#     subnet_info = hypertensor.get_formatted_all_subnet_info()
#     print(subnet_info)


# # pytest tests/substrate/test_rpc.py::test_subnets_data -rP


# def test_subnets_data():
#     subnets_data = hypertensor.get_subnet_data(1)
#     print(subnets_data)


# # pytest tests/substrate/test_rpc.py::test_get_subnet_node_info -rP


# def test_get_subnet_node_info():
#     rpc_runtime_config = RuntimeConfiguration()
#     rpc_runtime_config.update_type_registry(load_type_registry_preset("legacy"))
#     rpc_runtime_config.update_type_registry(custom_rpc_type_registry)

#     with hypertensor.interface as _interface:
#         result = _interface.rpc_request(method="network_getSubnetNodeInfo", params=[1, 1])
#         # print("Raw result:", result)
#         scale_obj = rpc_runtime_config.create_scale_object("Option<SubnetNodeInfo>")
#         type_info = scale_obj.generate_type_decomposition()
#         print("type_info", type_info)

#         if "result" in result and result["result"]:
#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object("Option<SubnetNodeInfo>")

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode(ScaleBytes(bytes(result["result"])))
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))

#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object(
#                     "Option<SubnetNodeInfo>", data=ScaleBytes(bytes(result["result"]))
#                 )

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode()
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))


# # pytest tests/substrate/test_rpc.py::test_get_subnet_node_info_formatted -rP


# def test_get_subnet_node_info_formatted():
#     subnet_node_info = hypertensor.get_formatted_get_subnet_node_info(1, 1)
#     print(subnet_node_info)


# # pytest tests/substrate/test_rpc.py::test_get_min_class_subnet_nodes_formatted -rP


# def test_get_min_class_subnet_nodes_formatted():
#     subnet_node_info = hypertensor.get_min_class_subnet_nodes_formatted(1, 0, SubnetNodeClass.Registered)
#     print(subnet_node_info)


# # pytest tests/substrate/test_rpc.py::test_get_subnet_nodes_info -rP


# def test_get_subnet_nodes_info():
#     rpc_runtime_config = RuntimeConfiguration()
#     rpc_runtime_config.update_type_registry(load_type_registry_preset("legacy"))
#     rpc_runtime_config.update_type_registry(custom_rpc_type_registry)

#     with hypertensor.interface as _interface:
#         result = _interface.rpc_request(method="network_getSubnetNodesInfo", params=[1])
#         # print("Raw result:", result)
#         scale_obj = rpc_runtime_config.create_scale_object("Vec<SubnetNodeInfo>")
#         type_info = scale_obj.generate_type_decomposition()
#         print("type_info", type_info)

#         if "result" in result and result["result"]:
#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object("Vec<SubnetNodeInfo>")

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode(ScaleBytes(bytes(result["result"])))
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))

#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object(
#                     "Vec<SubnetNodeInfo>", data=ScaleBytes(bytes(result["result"]))
#                 )

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode()
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))


# # pytest tests/substrate/test_rpc.py::test_get_subnet_nodes_info_formatted -rP


# def test_get_subnet_nodes_info_formatted():
#     subnet_node_info = hypertensor.get_subnet_nodes_info_formatted(1)
#     print(subnet_node_info)


# # pytest tests/substrate/test_rpc.py::test_get_all_subnet_nodes_info -rP


# def test_get_all_subnet_nodes_info():
#     rpc_runtime_config = RuntimeConfiguration()
#     rpc_runtime_config.update_type_registry(load_type_registry_preset("legacy"))
#     rpc_runtime_config.update_type_registry(custom_rpc_type_registry)

#     with hypertensor.interface as _interface:
#         result = _interface.rpc_request(method="network_getAllSubnetNodesInfo", params=[])
#         # print("Raw result:", result)
#         scale_obj = rpc_runtime_config.create_scale_object("Vec<SubnetNodeInfo>")
#         type_info = scale_obj.generate_type_decomposition()
#         print("type_info", type_info)

#         if "result" in result and result["result"]:
#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object("Vec<SubnetNodeInfo>")

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode(ScaleBytes(bytes(result["result"])))
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))

#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object(
#                     "Vec<SubnetNodeInfo>", data=ScaleBytes(bytes(result["result"]))
#                 )

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode()
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))


# # pytest tests/substrate/test_rpc.py::test_get_all_subnet_nodes_info_formatted -rP


# def test_get_all_subnet_nodes_info_formatted():
#     subnet_node_info = hypertensor.get_all_subnet_nodes_info_formatted()
#     print(subnet_node_info)


# # pytest tests/substrate/test_rpc.py::test_proof_of_stake -rP


# def test_proof_of_stake():
#     rpc_runtime_config = RuntimeConfiguration()
#     rpc_runtime_config.update_type_registry(load_type_registry_preset("legacy"))
#     rpc_runtime_config.update_type_registry(custom_rpc_type_registry)

#     peer_id_to_vec = [ord(char) for char in "12D1KooWGFuUunX1AzAzjs3CgyqTXtPWX3AqRhJFbesGPGYHJQTP"]

#     with hypertensor.interface as _interface:
#         result = _interface.rpc_request(
#             method="network_proofOfStake",
#             params=[
#                 1,
#                 peer_id_to_vec,
#                 1,
#             ],
#         )
#         print("result['result']", result["result"])


# python -m pytest tests/hypertensor/test_rpc.py::test_get_bootnodes -rP


def test_get_bootnodes():
    rpc_runtime_config = RuntimeConfiguration()
    rpc_runtime_config.update_type_registry(load_type_registry_preset("legacy"))
    rpc_runtime_config.update_type_registry(custom_rpc_type_registry)

    with hypertensor.interface as _interface:
        result = _interface.rpc_request(method="network_getBootnodes", params=[1])
        # print("Raw result:", result)
        scale_obj = rpc_runtime_config.create_scale_object("AllSubnetBootnodes")
        type_info = scale_obj.generate_type_decomposition()
        print("type_info", type_info)

        if "result" in result and result["result"]:
            try:
                # Create scale object for decoding
                obj = rpc_runtime_config.create_scale_object("AllSubnetBootnodes")

                # Decode the hex-encoded SCALE data (don't encode!)
                decoded_data = obj.decode(ScaleBytes(bytes(result["result"])))
                print("Decoded data:", decoded_data)

            except Exception as e:
                print("Decode error:", str(e))

            try:
                # Create scale object for decoding
                obj = rpc_runtime_config.create_scale_object(
                    "AllSubnetBootnodes", data=ScaleBytes(bytes(result["result"]))
                )

                # Decode the hex-encoded SCALE data (don't encode!)
                decoded_data = obj.decode()
                print("Decoded data:", decoded_data)

            except Exception as e:
                print("Decode error:", str(e))


# python -m pytest tests/hypertensor/test_rpc.py::test_get_all_overwatch_nodes_info -rP


def test_get_all_overwatch_nodes_info():
    rpc_runtime_config = RuntimeConfiguration()
    rpc_runtime_config.update_type_registry(load_type_registry_preset("legacy"))
    rpc_runtime_config.update_type_registry(custom_rpc_type_registry)

    with hypertensor.interface as _interface:
        result = _interface.rpc_request(method="network_getAllOverwatchNodesInfo", params=[])
        # print("Raw result:", result)
        scale_obj = rpc_runtime_config.create_scale_object("Vec<OverwatchNodeInfo>")
        type_info = scale_obj.generate_type_decomposition()
        print("type_info", type_info)

        if "result" in result and result["result"]:
            try:
                # Create scale object for decoding
                obj = rpc_runtime_config.create_scale_object("Vec<OverwatchNodeInfo>")

                # Decode the hex-encoded SCALE data (don't encode!)
                decoded_data = obj.decode(ScaleBytes(bytes(result["result"])))
                print("Decoded data:", decoded_data)

            except Exception as e:
                print("Decode error:", str(e))

            try:
                # Create scale object for decoding
                obj = rpc_runtime_config.create_scale_object(
                    "Vec<OverwatchNodeInfo>", data=ScaleBytes(bytes(result["result"]))
                )

                # Decode the hex-encoded SCALE data (don't encode!)
                decoded_data = obj.decode()
                print("Decoded data:", decoded_data)

            except Exception as e:
                print("Decode error:", str(e))


# python -m pytest tests/hypertensor/test_rpc.py::test_get_all_overwatch_nodes_info_formatted -rP


def test_get_all_overwatch_nodes_info_formatted():
    overwatch_nodes = hypertensor.get_all_overwatch_nodes_info_formatted()
    print("overwatch_nodes", overwatch_nodes)


# pytest tests/substrate/test_rpc.py::test_get_bootnodes_formatted -rP


# def test_get_bootnodes_formatted():
#     bootnodes = hypertensor.get_bootnodes_formatted(1)
#     print(bootnodes)


# # pytest tests/substrate/test_rpc.py::test_get_coldkey_subnet_nodes_info -rP


# def test_get_coldkey_subnet_nodes_info():
#     rpc_runtime_config = RuntimeConfiguration()
#     rpc_runtime_config.update_type_registry(load_type_registry_preset("legacy"))
#     rpc_runtime_config.update_type_registry(custom_rpc_type_registry)

#     with hypertensor.interface as _interface:
#         result = _interface.rpc_request(
#             method="network_getColdkeySubnetNodesInfo", params=["0xf24FF3a9CF04c71Dbc94D0b566f7A27B94566cac"]
#         )
#         # print("Raw result:", result)
#         scale_obj = rpc_runtime_config.create_scale_object("Vec<SubnetNodeInfo>")
#         type_info = scale_obj.generate_type_decomposition()
#         print("type_info", type_info)

#         print("result:", result)

#         if "result" in result and result["result"]:
#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object("Vec<SubnetNodeInfo>")

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode(ScaleBytes(bytes(result["result"])))
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))

#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object(
#                     "Vec<SubnetNodeInfo>", data=ScaleBytes(bytes(result["result"]))
#                 )

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode()
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))


# # pytest tests/substrate/test_rpc.py::test_get_coldkey_subnet_nodes_info_formatted -rP


# def test_get_coldkey_subnet_nodes_info_formatted():
#     data = hypertensor.get_coldkey_subnet_nodes_info_formatted("0xf24FF3a9CF04c71Dbc94D0b566f7A27B94566cac")
#     print(data)


# # pytest tests/substrate/test_rpc.py::test_get_coldkey_stakes -rP


# def test_get_coldkey_stakes():
#     rpc_runtime_config = RuntimeConfiguration()
#     rpc_runtime_config.update_type_registry(load_type_registry_preset("legacy"))
#     rpc_runtime_config.update_type_registry(custom_rpc_type_registry)

#     with hypertensor.interface as _interface:
#         result = _interface.rpc_request(
#             method="network_getColdkeyStakes", params=["0xf24FF3a9CF04c71Dbc94D0b566f7A27B94566cac"]
#         )
#         # print("Raw result:", result)
#         scale_obj = rpc_runtime_config.create_scale_object("Vec<SubnetNodeStakeInfo>")
#         type_info = scale_obj.generate_type_decomposition()
#         print("type_info", type_info)

#         print("result:", result)

#         if "result" in result and result["result"]:
#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object("Vec<SubnetNodeStakeInfo>")

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode(ScaleBytes(bytes(result["result"])))
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))

#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object(
#                     "Vec<SubnetNodeStakeInfo>", data=ScaleBytes(bytes(result["result"]))
#                 )

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode()
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))


# # pytest tests/substrate/test_rpc.py::test_get_coldkey_stakes_formatted -rP


# def test_get_coldkey_stakes_formatted():
#     data = hypertensor.get_coldkey_stakes_formatted("0xf24FF3a9CF04c71Dbc94D0b566f7A27B94566cac")
#     print(data)


# # pytest tests/substrate/test_rpc.py::test_get_delegate_stakes -rP


# def test_get_delegate_stakes():
#     rpc_runtime_config = RuntimeConfiguration()
#     rpc_runtime_config.update_type_registry(load_type_registry_preset("legacy"))
#     rpc_runtime_config.update_type_registry(custom_rpc_type_registry)

#     with hypertensor.interface as _interface:
#         result = _interface.rpc_request(
#             method="network_getDelegateStakes", params=["0xf24FF3a9CF04c71Dbc94D0b566f7A27B94566cac"]
#         )
#         # print("Raw result:", result)
#         scale_obj = rpc_runtime_config.create_scale_object("Vec<DelegateStakeInfo>")
#         type_info = scale_obj.generate_type_decomposition()
#         print("type_info", type_info)

#         print("result:", result)

#         if "result" in result and result["result"]:
#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object("Vec<DelegateStakeInfo>")

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode(ScaleBytes(bytes(result["result"])))
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))

#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object(
#                     "Vec<DelegateStakeInfo>", data=ScaleBytes(bytes(result["result"]))
#                 )

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode()
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))


# # pytest tests/substrate/test_rpc.py::test_get_delegate_stakes_formatted -rP


# def test_get_delegate_stakes_formatted():
#     data = hypertensor.get_delegate_stakes_formatted("0xf24FF3a9CF04c71Dbc94D0b566f7A27B94566cac")
#     print(data)


# # pytest tests/substrate/test_rpc.py::test_get_node_delegate_stakes -rP


# def test_get_node_delegate_stakes():
#     rpc_runtime_config = RuntimeConfiguration()
#     rpc_runtime_config.update_type_registry(load_type_registry_preset("legacy"))
#     rpc_runtime_config.update_type_registry(custom_rpc_type_registry)

#     with hypertensor.interface as _interface:
#         result = _interface.rpc_request(
#             method="network_getNodeDelegateStakes", params=["0xf24FF3a9CF04c71Dbc94D0b566f7A27B94566cac"]
#         )
#         # print("Raw result:", result)
#         scale_obj = rpc_runtime_config.create_scale_object("Vec<NodeDelegateStakeInfo>")
#         type_info = scale_obj.generate_type_decomposition()
#         print("type_info", type_info)

#         print("result:", result)

#         if "result" in result and result["result"]:
#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object("Vec<NodeDelegateStakeInfo>")

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode(ScaleBytes(bytes(result["result"])))
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))

#             try:
#                 # Create scale object for decoding
#                 obj = rpc_runtime_config.create_scale_object(
#                     "Vec<NodeDelegateStakeInfo>", data=ScaleBytes(bytes(result["result"]))
#                 )

#                 # Decode the hex-encoded SCALE data (don't encode!)
#                 decoded_data = obj.decode()
#                 print("Decoded data:", decoded_data)

#             except Exception as e:
#                 print("Decode error:", str(e))


# # pytest tests/substrate/test_rpc.py::test_get_node_delegate_stakes_formatted -rP


# def test_get_node_delegate_stakes_formatted():
#     data = hypertensor.get_node_delegate_stakes_formatted("0xf24FF3a9CF04c71Dbc94D0b566f7A27B94566cac")
#     print(data)


# # pytest tests/substrate/test_rpc.py::test_get_consensus_data_formatted -rP
# # NOTE: This requires consensus data to be present on the subnet


# def test_get_consensus_data_formatted():
#     # subnet_info = hypertensor.get_consensus_data_formatted(1, 15)
#     # print("subnet_info", subnet_info)
#     result = hypertensor.get_consensus_data(1, 1)

#     consensus_data = ConsensusData.fix_decoded_values(result)
#     print("consensus_data", consensus_data)


# # pytest tests/substrate/test_rpc.py::test_register_subnet -rP


# def test_register_subnet():
#     from scalecodec.types import CompactU32, Map

#     initial_coldkeys = [
#         ("0xf24FF3a9CF04c71Dbc94D0b566f7A27B94566cac", 1),
#         ("0x3Cd0A705a2DC65e5b1E1205896BaA2be8A07c6e0", 1),
#         ("0x798d4Ba9baf0064Ec19eB4F0a1a45785ae9D6DFc", 1),
#         ("0x773539d4Ac0e786233D90A233654ccEE26a613D9", 1),
#     ]

#     initial_coldkeys = sorted(set(initial_coldkeys), key=lambda x: x[0])

#     _orig_process_encode = Map.process_encode

#     def _patched_process_encode(self, value):
#         if type(value) is not list:
#             raise ValueError("value should be a list of tuples e.g.: [('1', 2), ('23', 24), ('28', 30), ('45', 80)]")

#         element_count_compact = CompactU32()
#         element_count_compact.encode(len(value))

#         data = element_count_compact.data

#         self.map_key = "[u8; 20]"
#         self.map_value = "u32"

#         for item_key, item_value in value:
#             key_obj = self.runtime_config.create_scale_object(type_string=self.map_key, metadata=self.metadata)

#             data += key_obj.encode(item_key)

#             value_obj = self.runtime_config.create_scale_object(type_string=self.map_value, metadata=self.metadata)

#             data += value_obj.encode(item_value)

#         return data

#     Map.process_encode = _patched_process_encode

#     call = hypertensor.interface.compose_call(
#         call_module="Network",
#         call_function="register_subnet",
#         call_params={
#             "max_cost": 100000000000000000000,
#             "subnet_data": {
#                 "name": "name-1".encode(),
#                 "repo": "repo".encode(),
#                 "description": "description".encode(),
#                 "misc": "misc".encode(),
#                 "min_stake": 100000000000000000000,
#                 "max_stake": 100000000000000000001,
#                 "delegate_stake_percentage": 100000000000000000,
#                 "initial_coldkeys": initial_coldkeys,
#                 "key_types": sorted(set(["Rsa"])),
#                 "bootnodes": sorted(set(["p2p/127.0.0.1/tcp"])),
#             },
#         },
#     )

#     # create signed extrinsic
#     extrinsic = hypertensor.interface.create_signed_extrinsic(call=call, keypair=hypertensor.keypair)

#     def submit_extrinsic():
#         try:
#             with hypertensor.interface as _interface:
#                 receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
#                 print("receipt is_success", receipt.is_success)
#                 return receipt
#         except SubstrateRequestException as e:
#             logger.error("Failed to send: {}".format(e))

#     try:
#         submit_extrinsic()
#     except Exception as e:
#         logger.error(f"test_btree_map {e}", exc_info=True)

#     # Reset library
#     Map.process_encode = _orig_process_encode


# # pytest tests/substrate/test_rpc.py::test_owner_add_or_update_initial_coldkeys -rP


# def test_owner_add_or_update_initial_coldkeys():
#     coldkeys = [
#         ("0xf24FF3a9CF04c71Dbc94D0b566f7A27B94566cac", "1"),
#         ("0x3Cd0A705a2DC65e5b1E1205896BaA2be8A07c6e0", "1"),
#         ("0x798d4Ba9baf0064Ec19eB4F0a1a45785ae9D6DFc", "1"),
#         ("0x773539d4Ac0e786233D90A233654ccEE26a613D9", "1"),
#     ]

#     coldkeys = sorted(set(coldkeys), key=lambda x: x[0])

#     try:
#         call = hypertensor.interface.compose_call(
#             call_module="Network",
#             call_function="owner_add_or_update_initial_coldkeys_v2",
#             call_params={
#                 "subnet_id": 1,
#                 "coldkeys": coldkeys,
#             },
#         )

#         # create signed extrinsic
#         extrinsic = hypertensor.interface.create_signed_extrinsic(call=call, keypair=hypertensor.keypair)

#         with hypertensor.interface as _interface:
#             receipt = _interface.submit_extrinsic(extrinsic, wait_for_inclusion=True)
#             print("receipt", receipt)
#             # if receipt.is_success:
#             #     print("is_success")
#             #     for event in receipt.triggered_events:
#             #         print(f'* {event.value}')
#             # return receipt
#     except Exception as e:
#         logger.error(f"test_btree_map {e}", exc_info=True)
