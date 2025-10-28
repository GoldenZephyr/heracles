#!/usr/bin/env python3
import argparse
import os
from importlib.resources import as_file, files

import spark_dsg
import yaml

import heracles
import heracles.resources
from heracles.dsg_utils import summarize_dsg
from heracles.graph_interface import (
    initialize_db,
    spark_dsg_to_db,
)
from heracles.query_interface import Neo4jWrapper


def main(scene_graph, neo4j_uri, neo4j_creds):
    global db # Global for easy access in interactive mode
    db = Neo4jWrapper(neo4j_uri, neo4j_creds, atomic_queries=True, print_profiles=False)
    db.connect()
    # Clear any existing content from the DB & initialize with the schema
    print("Initializing the database.")
    initialize_db(db)
    # Load the scene graph into the DB
    print("Loading the scene graph into the database.")
    spark_dsg_to_db(scene_graph, db)
    # Print the distinct object classes and their count
    print("Querying for all of the distinct object classes and their count.")
    records, summary, keys = db.execute(
        """MATCH (n: Object) RETURN DISTINCT n.class as class, COUNT(*) as count"""
    )
    for record in records:
        print(record.data())
    return


if __name__ == "__main__":
    # Check for DB authentication credentials
    neo4j_user = os.getenv("HERACLES_NEO4J_USERNAME")
    assert neo4j_user, '"$HERACLES_NEO4J_USERNAME" is not set.'
    neo4j_pw = os.getenv("HERACLES_NEO4J_PASSWORD")
    assert neo4j_pw, '"$HERACLES_NEO4J_PASSWORD" is not set.'
    neo4j_creds = (neo4j_user, neo4j_pw)
    # Parse the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scene_graph", type=str, default="scene_graphs/example_dsg.json"
    )
    parser.add_argument(
        "--neo4j_uri", type=str, default=os.getenv("HERACLES_NEO4J_URI")
    )
    parser.add_argument("--object_labelspace", type=str, default="")
    parser.add_argument("--room_labelspace", type=str, default="")
    args = parser.parse_args()
    assert args.neo4j_uri, (
        'No NEO4J_URI provided -- either provide as an arg or set "$HERACLES_NEO4J_URI"'
    )
    # Load the scene graph from file
    print(f'Loading the scene graph from file ("{args.scene_graph}").')
    scene_graph = spark_dsg.DynamicSceneGraph.load(args.scene_graph)
    print("Done loading the scene graph from file.")
    summarize_dsg(scene_graph)
    # Load the object labelspace
    if args.object_labelspace:
        with open(args.object_labelspace, "r") as file:
            object_labelspace = yaml.safe_load(file)
    else:
        with as_file(
            files(heracles.resources).joinpath("ade20k_mit_label_space.yaml")
        ) as path:
            with open(str(path), "r") as file:
                object_labelspace = yaml.safe_load(file)
    id_to_object_label = {
        item["label"]: item["name"] for item in object_labelspace["label_names"]
    }
    scene_graph.metadata.add({"labelspace": id_to_object_label})
    # Load the region/room labelspace
    if args.room_labelspace:
        with open(args.room_labelspace, "r") as file:
            room_labelspace = yaml.safe_load(file)
    else:
        with as_file(
            files(heracles.resources).joinpath("b45_label_space.yaml")
        ) as path:
            with open(str(path), "r") as file:
                room_labelspace = yaml.safe_load(file)
    id_to_room_label = {
        item["label"]: item["name"] for item in room_labelspace["label_names"]
    }
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
    main(scene_graph, args.neo4j_uri, neo4j_creds)
