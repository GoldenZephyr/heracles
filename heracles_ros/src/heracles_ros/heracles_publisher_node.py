#!/usr/bin/env python3

import logging
import os
from importlib.resources import as_file, files

import heracles.constants
import heracles.resources
import numpy as np
import rclpy
import spark_dsg
import yaml
from heracles.dsg_utils import summarize_dsg
from heracles.graph_interface import db_to_spark_dsg
from heracles.query_interface import Neo4jWrapper
from rclpy.node import Node

from heracles_ros.hydra_python_publisher import DsgPublisher

logger = logging.getLogger(__name__)


def center_w_h_to_bb(center, w, h):
    return np.array(
        [
            [center[0] - w, center[1] - h],
            [center[0] + w, center[1] - h],
            [center[0] + w, center[1] + h],
            [center[0] - w, center[1] + h],
        ]
    )


def load_abs_or_pkg_path_as_yaml(pkg, path):
    if os.path.exists(path):
        full_path = path
    else:
        with as_file(files(pkg).joinpath(path)) as p:
            full_path = str(p)

    with open(full_path, "r") as fo:
        data = yaml.safe_load(fo)
    return data


class HeraclesPublisher(Node):
    def __init__(self):
        super().__init__("heracles_publisher")

        # Declare and get parameters
        self.declare_parameter("heracles_ip", "")
        self.declare_parameter("heracles_port", -1)
        self.declare_parameter("heracles_neo4j_user", "neo4j")
        self.declare_parameter("heracles_neo4j_pass", "neo4j_pw")
        self.declare_parameter("object_labelspace", "")
        self.declare_parameter("region_labelspace", "")

        ip = self.get_parameter("heracles_ip").get_parameter_value().string_value
        port = self.get_parameter("heracles_port").get_parameter_value().integer_value
        self.get_logger().info(f"Port: {port}")

        assert ip != "", "Please set database IP"
        assert port > 0, "Please set database port"

        # IP / Port for database
        self.URI = f"neo4j://{ip}:{port}"
        self.get_logger().info(f"Connecting to {self.URI}")
        # Database name / password for database
        user = (
            self.get_parameter("heracles_neo4j_user").get_parameter_value().string_value
        )
        pw = (
            self.get_parameter("heracles_neo4j_pass").get_parameter_value().string_value
        )
        self.AUTH = (user, pw)

        self.layer_id_to_layer_name = None
        self.object_label_to_id = None
        self.room_label_to_id = None

        # NOTE: Ideally we would store the labelspaces in the database so that we don't need to track this extra metadata
        object_labelspace_name = (
            self.get_parameter("object_labelspace").get_parameter_value().string_value
        )
        assert object_labelspace_name != "", "Please set `object_labelspace`"
        region_labelspace_name = (
            self.get_parameter("region_labelspace").get_parameter_value().string_value
        )
        assert region_labelspace_name != "", "Please set `reigon_labelspace`"
        self.setup_labelspaces(object_labelspace_name, region_labelspace_name)

        self.dsg_sender = DsgPublisher(self, "~/dsg_out", True)
        # Timer to publish DSG at regular intervals
        timer_period_s = 5
        self.timer = self.create_timer(timer_period_s, self.publish_dsg)

    def setup_labelspaces(self, object_labelspace_name, region_labelspace_name):
        object_labelspace = load_abs_or_pkg_path_as_yaml(
            heracles.resources, object_labelspace_name
        )

        id_to_object_label = {
            item["label"]: item["name"] for item in object_labelspace["label_names"]
        }

        self.get_logger().info(f"region_labelspace_name: {region_labelspace_name}")
        room_labelspace = load_abs_or_pkg_path_as_yaml(
            heracles.resources, region_labelspace_name
        )

        id_to_room_label = {
            item["label"]: item["name"] for item in room_labelspace["label_names"]
        }
        self.get_logger().info(f"id_to_room_label: {id_to_room_label}")

        # Define a layer id map
        self.layer_id_to_layer_name = {
            20: heracles.constants.MESH_PLACES,
            3: heracles.constants.PLACES,
            2: heracles.constants.OBJECTS,
            4: heracles.constants.ROOMS,
            5: heracles.constants.BUILDINGS,
        }

        self.object_label_to_id = {v: k for k, v in id_to_object_label.items()}
        self.room_label_to_id = {v: k for k, v in id_to_room_label.items()}

    def publish_dsg(self):
        with Neo4jWrapper(
            self.URI, self.AUTH, atomic_queries=True, print_profiles=False
        ) as db:
            new_scene_graph = db_to_spark_dsg(
                db,
                self.layer_id_to_layer_name,
                self.object_label_to_id,
                self.room_label_to_id,
            )
        summarize_dsg(new_scene_graph)

        for n in new_scene_graph.get_layer(spark_dsg.DsgLayers.MESH_PLACES).nodes:
            n.attributes.bounding_box = spark_dsg.BoundingBox(
                (1, 1, 0.001), n.attributes.position
            )
            corners = center_w_h_to_bb(n.attributes.position, 1, 1)
            boundary = np.zeros((4, 3))
            boundary[:, :2] = corners
            boundary[:, 2] = n.attributes.position[2]
            n.attributes.boundary = boundary

        self.dsg_sender.publish(new_scene_graph, frame_id="map")
        self.get_logger().info("Published map")


def main(args=None):
    rclpy.init(args=args)

    # Create the node
    node = HeraclesPublisher()

    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        # Clean up before shutdown
        node.destroy_node()
        rclpy.shutdown()


if __name__ == "__main__":
    main()
