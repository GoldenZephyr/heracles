#!/usr/bin/env python3
from importlib_resources import as_file, files
import sys
import time

import zmq

import spark_dsg
import yaml
import neo4j

import heracles
from heracles.dsg_utils import summarize_dsg
import heracles.resources
from heracles.query_interface import Neo4jWrapper
from heracles.graph_interface import (
    add_objects_from_dsg,
    add_places_from_dsg,
    add_mesh_places_from_dsg,
    add_rooms_from_dsg,
    add_buildings_from_dsg,
    add_edges_from_dsg,
    db_to_spark_dsg,
)

def try_drop_index(db, index_name):

    try:
        db.execute(f"DROP INDEX {index_name}")
    except neo4j.exceptions.DatabaseError:
        print(f"No index `{index_name}`")


# IP / Port for database
URI = "neo4j://127.0.0.1:7687"
# Database name / password for database
AUTH = ("neo4j", "neo4j_pw")

# Load a scene graph from file
if len(sys.argv) > 1:
    print(f"Trying to load {sys.argv[1]}")
    G = spark_dsg.DynamicSceneGraph.load(sys.argv[1])
    print("Success!")
else:
    fn = "/heracles/src/heracles/resources/scene_graphs/dsg_from_jared_0.json"
    print(f"Trying to load {fn}")
    G = spark_dsg.DynamicSceneGraph.load(fn)
    print("Success!")

summarize_dsg(G)

if G.metadata == {}:
    # Load the object semantic labels
    with open("/heracles/src/heracles/resources/label_spaces/ade20k_mit_label_space.yaml") as f:
        object_labelspace = yaml.safe_load(f)
    id_to_label = {item["label"]: item["name"] for item in object_labelspace["label_names"]}
    G.add_metadata({"labelspace": id_to_label})

    # Load the room semantic labels
    with open("/heracles/src/heracles/resources/label_spaces/scene_courtyard_label_space.yaml") as f:
        room_labelspace = yaml.safe_load(f)
    room_id_to_label = {item["label"] : item["name"] for item in room_labelspace["label_names"]}
    G.add_metadata({"room_labelspace": room_id_to_label})

layers = {
    spark_dsg.DsgLayers.OBJECTS: "Object",
    spark_dsg.DsgLayers.BUILDINGS: "Building",
    spark_dsg.DsgLayers.MESH_PLACES: "MeshPlace",
    spark_dsg.DsgLayers.PLACES: "Place",
    spark_dsg.DsgLayers.ROOMS: "Room",
}
G.add_metadata({"LayerIdToLayerStr": layers})

with Neo4jWrapper(URI, AUTH, atomic_queries=True, print_profiles=False) as db:

    # Initialize the db for a scene graph
    db.execute(
        "MATCH (n) DETACH DELETE n",
    )

    try_drop_index(db, "object_node_symbol")
    try_drop_index(db, "place_node_symbol")
    try_drop_index(db, "mesh_place_node_symbol")
    try_drop_index(db, "room_node_symbol")

    db.execute(
        """
    CREATE INDEX object_node_symbol FOR (n:Object) ON (n.nodeSymbol)
    """
    )

    db.execute(
        """
    CREATE INDEX place_node_symbol FOR (n:Place) ON (n.nodeSymbol)
    """
    )

    db.execute(
        """
    CREATE INDEX mesh_place_node_symbol FOR (n:MeshPlace) ON (n.nodeSymbol)
    """
    )

    db.execute(
        """
    CREATE INDEX room_node_symbol FOR (n:Room) ON (n.nodeSymbol)
    """
    )

    # Populate the db with scene graph information, excluding rooms
    add_objects_from_dsg(G, db)
    add_places_from_dsg(G, db)
    add_mesh_places_from_dsg(G, db)
    add_buildings_from_dsg(G, db)
    add_edges_from_dsg(G, db)
    # Convert from the db back to spark
    label_to_id = {v : k for k,v in id_to_label.items()}
    new_G = db_to_spark_dsg(db, label_to_id, label_to_id)
    # Visualize the scene graph
    viz = spark_dsg.open3d_visualization.DsgVisualizer(url="tcp://127.0.0.1:8001")
    time.sleep(0.5)
    viz.update_graph(new_G, force=True)

    # Pause, then add the rooms and update the visualization
    input("Press enter to update the scene graph")
    add_rooms_from_dsg(G, db)
    add_edges_from_dsg(G, db)
    new_G = db_to_spark_dsg(db, label_to_id, label_to_id)
    viz.update_graph(new_G, force=True)

    # By creating the DsgVisualizer here, we can directly control updates. Below is how to do render a scene graph once.
    # spark_dsg.render_to_open3d(new_G, block=False)
