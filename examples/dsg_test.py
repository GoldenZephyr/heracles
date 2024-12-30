#!/usr/bin/env python3
import spark_dsg
from pydsg.pydsg_translator import spark_dsg_to_pydsg
from pydsg.dsg_plotter import plot_dsg_places, plot_dsg_objects
import heracles
from heracles.dsg_utils import summarize_dsg
import heracles.resources
import yaml
from importlib.resources import as_file, files
import neo4j

import matplotlib.pyplot as plt

from heracles.query_interface import Neo4jWrapper

from heracles.graph_interface import (
    add_objects_from_dsg,
    add_places_from_dsg,
    add_mesh_places_from_dsg,
    add_rooms_from_dsg,
    add_buildings_from_dsg,
    add_edges_from_dsg,
)

import time


def try_drop_index(db, index_name):

    try:
        db.execute(f"DROP INDEX {index_name}")
    except neo4j.exceptions.DatabaseError as ex:
        print(f"No index `{index_name}`")


URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "neo4jiscool")

# TODO: write this to graph
layers = {
    spark_dsg.DsgLayers.OBJECTS: "Object",
    spark_dsg.DsgLayers.BUILDINGS: "Building",
    spark_dsg.DsgLayers.MESH_PLACES: "MeshPlace",
    spark_dsg.DsgLayers.PLACES: "Place",
    spark_dsg.DsgLayers.ROOMS: "Room",
}

# G = spark_dsg.DynamicSceneGraph.load("scene_graph_full_loop_2.json")
G = spark_dsg.DynamicSceneGraph.load("t3_w0_ths2_fused.json")


summarize_dsg(G)


if G.metadata == {}:
    with as_file(
        files(heracles.resources).joinpath("ade20k_mit_label_space.yaml")
    ) as path:
        with open(str(path), "r") as fo:
            labelspace = yaml.safe_load(fo)
    id_to_label = {item["label"]: item["name"] for item in labelspace["label_names"]}
    G.add_metadata({"labelspace": id_to_label})

G.add_metadata({"LayerIdToLayerStr": layers})

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

    # pdsg = spark_dsg_to_pydsg(G)
    # plot_dsg_objects(pdsg)
    # plot_dsg_places(pdsg)
    # plt.show()
    # plt.savefig("pics/objs1.png")
