from typing import Optional

import rclpy
from builtin_interfaces.msg import Time
from hydra_msgs.msg import DsgUpdate
from rclpy.node import Node
from rclpy.qos import QoSDurabilityPolicy, QoSHistoryPolicy, QoSProfile
from std_msgs.msg import Header


class DsgPublisher:
    """Class for publishing a scene graph from python."""

    def __init__(self, node: Node, topic: str, publish_mesh: bool = True):
        """Construct a sender."""
        self._publish_mesh = publish_mesh

        qos_profile = QoSProfile(
            history=QoSHistoryPolicy.KEEP_ALL,
            depth=10,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
        )

        self._pub = node.create_publisher(DsgUpdate, topic, qos_profile)

    def publish(
        self, G, stamp: Optional[rclpy.time.Time] = None, frame_id: str = "odom"
    ):
        """Send a graph."""
        now = stamp if stamp is not None else rclpy.clock.Clock().now()
        header = Header()
        header.stamp = Time(
            sec=now.nanoseconds // 10**9, nanosec=now.nanoseconds % 10**9
        )
        header.frame_id = frame_id
        self.publish_with_header(G, header)

    def publish_with_header(self, G, header: Header):
        """Send a graph."""
        msg = DsgUpdate()
        msg.header = header
        msg.layer_contents = G.to_binary(self._publish_mesh)
        msg.full_update = True
        self._pub.publish(msg)
