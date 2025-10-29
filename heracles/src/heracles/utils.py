import os
from importlib.resources import as_file, files

import yaml

import heracles
import heracles.resources
from heracles.graph_interface import (
    initialize_db,
    spark_dsg_to_db,
)
from heracles.query_interface import Neo4jWrapper


def get_labelspace(labelspace_name):
    if os.path.exists(labelspace_name):
        with open(labelspace_name, "r") as file:
            labelspace = yaml.safe_load(file)
    else:
        with as_file(files(heracles.resources).joinpath(labelspace_name)) as path:
            with open(str(path), "r") as file:
                labelspace = yaml.safe_load(file)
    id_to_label = {item["label"]: item["name"] for item in labelspace["label_names"]}
    return id_to_label


def load_dsg_to_db(
    object_labelspace, room_labelspace, neo4j_uri, neo4j_creds, scene_graph
):
    id_to_object_label = get_labelspace(object_labelspace)
    scene_graph.metadata.add({"labelspace": id_to_object_label})

    id_to_room_label = get_labelspace(room_labelspace)
    scene_graph.metadata.add({"room_labelspace": id_to_room_label})

    # Set the layer id to layer name mappings
    spark_layer_id_to_heracles_layer_str = {
        2: "Object",
        5: "Building",
        20: "MeshPlace",
        "3[1]": "MeshPlace",
        3: "Place",
        4: "Room",
    }
    scene_graph.metadata.add(
        {"LayerIdToHeraclesLayerStr": spark_layer_id_to_heracles_layer_str}
    )

    with Neo4jWrapper(
        neo4j_uri, neo4j_creds, atomic_queries=True, print_profiles=False
    ) as db:
        # Clear any existing content from the DB & initialize with the schema
        print("Initializing the database.")
        initialize_db(db)
        # Load the scene graph into the DB
        print("Loading the scene graph into the database.")
        spark_dsg_to_db(scene_graph, db)
