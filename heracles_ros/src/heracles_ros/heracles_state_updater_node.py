#!/usr/bin/env python3

import rclpy
from rclpy.node import Node
from geometry_msgs.msg import TransformStamped
import tf2_ros

from heracles_agents.dsg_interfaces import HeraclesDsgInterface
from heracles.query_interface import Neo4jWrapper


class HeraclesPosePublisher(Node):
    def __init__(self):
        super().__init__("heracles_pose_publisher")

        self.declare_parameter("map_frame", "map")
        self.declare_parameter("robot_name", "hamilton")
        self.declare_parameter("publish_rate", 1.0)
        self.map_frame = self.get_parameter("map_frame").value
        self.robot_name = self.get_parameter("robot_name").value
        self.publish_rate = self.get_parameter("publish_rate").value

        self.dsgdb_conf = HeraclesDsgInterface(
            dsg_interface_type="heracles",
            uri="neo4j://$ADT4_HERACLES_IP:$ADT4_HERACLES_PORT"
        )

        self.tf_buffer = tf2_ros.Buffer()
        self.tf_listener = tf2_ros.TransformListener(self.tf_buffer, self)
        self.timer = self.create_timer(1.0 / self.publish_rate, self.timer_callback)

    def timer_callback(self):
        target_frame = f"{self.robot_name}/base_link"

        try:
            transform: TransformStamped = self.tf_buffer.lookup_transform(
                self.map_frame, target_frame, rclpy.time.Time()
            )
        except Exception as e:
            self.get_logger().warn(f"TF not available yet: {e}")
            return

        t = transform.transform.translation
        x, y, z = t.x, t.y, t.z
        q = transform.transform.rotation
        qw, qx, qy, qz = q.w, q.x, q.y, q.z

        query = f"""
            MERGE (r:Robot {{name: '{self.robot_name}'}})
            SET r.position = point({{x: {x}, y: {y}, z: {z}}}),
                r.qw = {qw},
                r.qx = {qx},
                r.qy = {qy},
                r.qz = {qz}
            RETURN r
        """
        with Neo4jWrapper(
            self.dsgdb_conf.uri,
            (
                self.dsgdb_conf.username.get_secret_value(),
                self.dsgdb_conf.password.get_secret_value(),
            ),
            atomic_queries=True,
            print_profiles=False,
        ) as db:
            db.query(query)

            self.get_logger().debug(
                f"Updating DB: {self.robot_name} pos=({x:.2f},{y:.2f},{z:.2f})"
            )


def main(args=None):
    rclpy.init(args=args)
    node = HeraclesPosePublisher()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
