#!/usr/bin/env python3
from importlib.resources import as_file, files
import sys
import time
import math

import spark_dsg
import yaml
import neo4j

import heracles
from heracles.dsg_utils import summarize_dsg
import heracles.resources
import heracles.constants
from heracles.query_interface import Neo4jWrapper
from heracles.graph_interface import (
    initialize_db,
    spark_dsg_to_db,
    db_to_spark_dsg,
)

def compare_objects(object_a, object_b):
    assert(object_a.id == object_b.id)
    assert(object_a.layer == object_b.layer)
    attr_a = object_a.attributes
    attr_b = object_b.attributes
    # position
    assert(math.isclose(attr_a.position.all(), attr_b.position.all()))
    # bbox (dim/pos)
    assert(math.isclose(attr_a.bounding_box.dimensions.all(), attr_b.bounding_box.dimensions.all()))
    assert(math.isclose(attr_a.bounding_box.world_P_center.all(), attr_b.bounding_box.world_P_center.all()))
    # color
    assert(attr_a.color.all() == attr_b.color.all())
    # semantic_label
    assert(attr_a.semantic_label == attr_b.semantic_label)
    # SemanticAttributeNode::name
    assert(attr_a.name == attr_b.name)

    
def compare_spark_dsgs(graph_a, graph_b):
    # Check that each layer has the same number of nodes & edges
    for layer_a in graph_a.layers:
        layer_b = graph_b.get_layer(layer_a.id)
        assert(layer_a.num_nodes() == layer_b.num_nodes())
        assert(layer_a.num_edges() == layer_b.num_edges())


    # Enforce that each layer has the same number of nodes
    # Compare the object nodes
    for object_a in graph_a.get_layer(spark_dsg.DsgLayers.OBJECTS).nodes:
        compare_objects(object_a, graph_b.get_node(object_a.id))
    return True

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
    import os
    fn = os.getenv("CONVERSION_TEST_DSG_FILENAME")
    print(f"Trying to load {fn}")
    original_scene_graph = spark_dsg.DynamicSceneGraph.load(fn)
    print("Success!")
# Summarize the loaded scene graph
summarize_dsg(original_scene_graph)

# Load the object and room/region labelspaces
with as_file(files(heracles.resources).joinpath("ade20k_mit_label_space.yaml")) as path:
    with open(str(path), "r") as fo:
        object_labelspace = yaml.safe_load(fo)
id_to_object_label = {item["label"]: item["name"] for item in object_labelspace["label_names"]}
original_scene_graph.metadata.add({"labelspace": id_to_object_label})

#with as_file(files(heracles.resources).joinpath("scene_camp_buckner_label_space.yaml")) as path:
with as_file(files(heracles.resources).joinpath("scene_courtyard_label_space.yaml")) as path:
    with open(str(path), "r") as fo:
        room_labelspace = yaml.safe_load(fo)
id_to_room_label = {item["label"]: item["name"] for item in room_labelspace["label_names"]}
original_scene_graph.metadata.add({"room_labelspace": id_to_room_label})

# Define a layer id map
layer_id_to_layer_name = {
    20: heracles.constants.MESH_PLACES,
    3: heracles.constants.PLACES,
    2: heracles.constants.OBJECTS,
    4: heracles.constants.ROOMS,
    5: heracles.constants.BUILDINGS
}
print(layer_id_to_layer_name)

original_scene_graph.metadata.add({"LayerIdToHeraclesLayerStr": layer_id_to_layer_name})

# Connect to the database and do some scene graph conversions
with Neo4jWrapper(URI, AUTH, atomic_queries=True, print_profiles=False) as db:
    initialize_db(db)
    # Convert from spark to the db
    spark_dsg_to_db(original_scene_graph, db)
    # Convert from the db back to spark
    object_label_to_id = {v : k for k,v in id_to_object_label.items()}
    room_label_to_id = {v : k for k,v in id_to_room_label.items()}
    new_scene_graph = db_to_spark_dsg(db, layer_id_to_layer_name, object_label_to_id, room_label_to_id)
    assert(compare_spark_dsgs(original_scene_graph, new_scene_graph))
    print("Conversion Test Passed!")
