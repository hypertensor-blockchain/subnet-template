from libp2p.utils.address_validation import get_available_interfaces
from multiaddr import Multiaddr


def get_public_ip_interfaces(
    ip: str, port: int, protocol: str = "tcp", current_interfaces: list[Multiaddr] | None = None
) -> list[Multiaddr]:
    """
    Returns a list of available interfaces with the forced public IP at the start.
    Ensures that no duplicate addresses are present in the final list.

    :param ip: The public IP address to force.
    :param port: Port number to bind to.
    :param protocol: Transport protocol (e.g., "tcp" or "udp").
    :param current_interfaces: Optional list of interfaces to include. If None,
                              will call get_available_interfaces.
    :return: List of Multiaddr objects starting with the public IP.
    """
    # Create the forced public IP multiaddress
    public_addr = Multiaddr(f"/ip4/{ip}/{protocol}/{port}")

    # If no interfaces provided, fetch them from get_available_interfaces
    if current_interfaces is None:
        current_interfaces = get_available_interfaces(port, protocol)

    # Place public_addr at the start
    interfaces: list[Multiaddr] = [public_addr]

    # Track seen addresses to avoid duplicates (using string representation for safety)
    seen = {str(public_addr)}

    for addr in current_interfaces:
        addr_str = str(addr)
        if addr_str not in seen:
            interfaces.append(addr)
            seen.add(addr_str)

    return interfaces
