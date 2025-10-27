#!/usr/bin/env python3
import sys
import time
from importlib.resources import as_file, files

import neo4j
import spark_dsg
import yaml

import heracles
import heracles.resources
from heracles.dsg_utils import summarize_dsg
from heracles.graph_interface import (
    add_buildings_from_dsg,
    add_edges_from_dsg,
    add_mesh_places_from_dsg,
    add_objects_from_dsg,
    add_places_from_dsg,
    add_rooms_from_dsg,
)
from heracles.query_interface import Neo4jWrapper


def try_drop_index(db, index_name):
    try:
        db.execute(f"DROP INDEX {index_name}")
    except neo4j.exceptions.DatabaseError:
        print(f"No index `{index_name}`")


# IP / Port for database
URI = "neo4j://127.0.0.1:7687"
# Database name / password for database
AUTH = ("neo4j", "neo4j_pw")


if len(sys.argv) > 1:
    print(f"Trying to load {sys.argv[1]}")
    G = spark_dsg.DynamicSceneGraph.load(sys.argv[1])
    print("Success!")
else:
    import os

    fn = os.getenv("CONVERSION_TEST_DSG_FILENAME")
    print(f"Trying to load {fn}")
    G = spark_dsg.DynamicSceneGraph.load(fn)
    print("Success!")


summarize_dsg(G)


with as_file(files(heracles.resources).joinpath("ade20k_mit_label_space.yaml")) as path:
    with open(str(path), "r") as fo:
        labelspace = yaml.safe_load(fo)
id_to_label = {item["label"]: item["name"] for item in labelspace["label_names"]}
G.metadata.add({"labelspace": id_to_label})

# region_ls = {
# 0:"unknown",
# 1:"sidewalk",
# 2:"road",
# 3:"courtyard",
# 4:"offroad",
# 5:"field",
# 6:"building",
# }

region_ls = {0: "parking", 1: "coutyard", 2: "alley", 3: "road", 4: "meadow", 5: "lawn"}

G.metadata.add({"room_labelspace": region_ls})


layers = {
    2: "Object",
    5: "Building",
    20: "MeshPlace",
    "3[1]": "MeshPlace",
    3: "Place",
    4: "Room",
}

G.metadata.add({"LayerIdToHeraclesLayerStr": layers})


with Neo4jWrapper(URI, AUTH, atomic_queries=True, print_profiles=False) as db:
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

    t0 = time.time()
    add_objects_from_dsg(G, db)
    add_places_from_dsg(G, db)
    add_mesh_places_from_dsg(G, db)
    add_rooms_from_dsg(G, db)
    add_buildings_from_dsg(G, db)

    print("Node insertion took %f seconds" % (time.time() - t0))

    add_edges_from_dsg(G, db)

    print("Full insertion took %f seconds" % (time.time() - t0))

    # find the boxes
    records, summary, keys = db.execute(
        """MATCH (p:Object {class: "box"})
        RETURN p.nodeSymbol AS nodeSymbol, p.class AS class, p.center AS center""",
    )

    # Loop through results and do something with them
    for record in records:
        print(record.data())

    # find the box closest to the origin
    records, summary, keys = db.execute(
        """
        MATCH (p:Object)
        WHERE p.class IN ["box", "sign"]
        WITH p, point.distance(point({x: 0, y: 0, z: 0}), p.center) AS dist
        RETURN p.nodeSymbol AS nodeSymbol, p.class AS class, dist, p.center AS center
        ORDER BY dist ASC
        LIMIT 1
        """,
    )

    print("Closest box/sign to origin:")
    # Loop through results and do something with them
    for record in records:
        print(record.data())
