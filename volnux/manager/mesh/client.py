import asyncio
import logging
from typing import Dict, List, Any
from volnux.signal.signals import config_changed

logger = logging.getLogger(__name__)


class MeshClient:
    def __init__(
        self, node_id: str, peers: List[str], config_loader: "VolnuxConfigLoader"
    ):
        self.node_id = node_id
        self.peers = peers  # List of gRPC/TCP addresses
        self.config = config_loader
        self.lock = asyncio.Lock()

        # Subscribe to local config changes to trigger Gossip
        config_changed.connect(self.on_local_config_update)

    async def broadcast_update(self, key: str, value: Any):
        """Broadcasts a local change to all known peers."""
        async with self.lock:
            # Increment logical clock for a new local event
            self.config._lamport_clock += 1

            payload = {
                "key": key,
                "value": value,
                "timestamp": self.config._lamport_clock,
                "origin_node": self.node_id,
                "signature": self._sign_payload(key, value),  # Security Layer
            }

        for peer in self.peers:
            asyncio.create_task(self._send_to_peer(peer, payload))

    async def handle_incoming_gossip(self, payload: Dict[str, Any]):
        """Called by the gRPC/TCP server when a peer sends an update."""
        key = payload["key"]
        incoming_entry = ConfigEntry(
            value=payload["value"],
            timestamp=payload["timestamp"],
            origin_node=payload["origin_node"],
            signature=payload["signature"],
        )

        # Use the tie-breaker logic in the ConfigLoader
        updated = self.config.update_from_mesh(key, incoming_entry)

        if updated:
            logger.info(f"Mesh Update Accepted: {key} from {payload['origin_node']}")
            # Trigger local signals so running events can 'Hot Reload' if needed
            await self._notify_internal_systems(key)

    async def _send_to_peer(self, peer_addr: str, payload: Dict[str, Any]):
        """Low-level network call (e.g., gRPC stub or TCP write)."""
        try:
            # Placeholder for actual gRPC/TCP logic
            # await grpc_stub.UpdateConfig(payload)
            pass
        except Exception as e:
            logger.error(f"Failed to push gossip to {peer_addr}: {e}")

    def _sign_payload(self, key: str, value: Any) -> bytes:
        # Placeholder for Ed25519 signing logic
        return b"sig_placeholder"
