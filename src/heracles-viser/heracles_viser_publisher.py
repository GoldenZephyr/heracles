"""Entry points for hercales visualizer."""
import argparse
import os
import time
import spark_dsg
import yaml
from importlib.resources import as_file, files

import heracles.resources
from heracles.dsg_utils import summarize_dsg
from heracles.graph_interface import db_to_spark_dsg
from heracles.query_interface import Neo4jWrapper

from spark_dsg.commands.visualize import ViserRenderer

def load_abs_or_pkg_path_as_yaml(pkg, path):
    if os.path.exists(path):
        full_path = path
    else:
        with as_file(files(pkg).joinpath(path)) as p:
            full_path = str(p)

    with open(full_path, "r") as fo:
        data = yaml.safe_load(fo)
    return data

def setup_labelspaces(object_labelspace_name, region_labelspace_name):
        object_labelspace = load_abs_or_pkg_path_as_yaml(
            heracles.resources, object_labelspace_name
        )

        id_to_object_label = {
            item["label"]: item["name"] for item in object_labelspace["label_names"]
        }

        room_labelspace = load_abs_or_pkg_path_as_yaml(
            heracles.resources, region_labelspace_name
        )

        id_to_room_label = {
            item["label"]: item["name"] for item in room_labelspace["label_names"]
        }

        # Define a layer id map
        layer_id_to_layer_name = {
            20: heracles.constants.MESH_PLACES,
            3: heracles.constants.PLACES,
            2: heracles.constants.OBJECTS,
            4: heracles.constants.ROOMS,
            5: heracles.constants.BUILDINGS,
        }

        object_label_to_id = {v: k for k, v in id_to_object_label.items()}
        room_label_to_id = {v: k for k, v in id_to_room_label.items()}
        return layer_id_to_layer_name, object_label_to_id, room_label_to_id

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", default="localhost")
    parser.add_argument("--port", default="8080", type=int)
    parser.add_argument("--uri", default="neo4j://127.0.0.1:7683")
    parser.add_argument("--user", default="neo4j")
    parser.add_argument("--pswd", default="neo4j_pw")
    #parser.add_argument("object_labelspace")
    #parser.add_argument("region_labelspace")

    args = parser.parse_args()
    renderer = ViserRenderer(args.ip, port=args.port)

    object_labelspace = "ade20k_mit_label_space.yaml"
    region_labelspace = "b45_label_space.yaml"

    auth = (args.user, args.pswd)
    layer_id_to_layer_name, object_label_to_id, room_label_to_id = setup_labelspaces(object_labelspace, region_labelspace)
    while True:
        with Neo4jWrapper(
            args.uri, auth, atomic_queries=True, print_profiles=False
        ) as db:
            new_scene_graph = db_to_spark_dsg(
                db,
                layer_id_to_layer_name,
                object_label_to_id,
                room_label_to_id,
            )
        renderer.draw(new_scene_graph)
        time.sleep(0.1)
        renderer.update()

        # update ever 5s
        time.sleep(5)



