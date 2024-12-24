#!/usr/bin/env python3
import importlib
import spark_dsg
from pydsg.pydsg_translator import spark_dsg_to_pydsg
from pydsg.dsg_plotter import plot_dsg_places, plot_dsg_objects
from neo4j import GraphDatabase
import heracles
import heracles.resources
import yaml
from importlib.resources import as_file, files

from heracles.graph_interface import (
    add_objects_to_neo4j,
    add_places_to_neo4j,
    add_mesh_places_to_neo4j,
    add_rooms_to_neo4j,
    add_buildings_to_neo4j,
)

URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "neo4jiscool")

# TODO: write this to graph
layers = {
    spark_dsg.DsgLayers.AGENTS: "agent",
    spark_dsg.DsgLayers.OBJECTS: "object",
    spark_dsg.DsgLayers.BUILDINGS: "building",
    spark_dsg.DsgLayers.MESH_PLACES: "mesh_place",
    spark_dsg.DsgLayers.PLACES: "place",
    spark_dsg.DsgLayers.ROOMS: "room",
    spark_dsg.DsgLayers.STRUCTURE: "structure",
}

G = spark_dsg.DynamicSceneGraph.load("scene_graph_full_loop_2.json")

if G.metadata == {}:
    with as_file(
        files(heracles.resources).joinpath("ade20k_mit_label_space.yaml")
    ) as path:
        # with importlib.resources.path(
        #    heracles.resources, "ade20k_mit_label_space.yaml"
        # ) as path:
        with open(str(path), "r") as fo:
            labelspace = yaml.safe_load(fo)
    id_to_label = {item["label"]: item["name"] for item in labelspace["label_names"]}
    G.add_metadata({"labelspace": id_to_label})


with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    driver.execute_query(
        "MATCH (n) DETACH DELETE n",
        database_="neo4j",
    )

    add_objects_to_neo4j(G, driver)
    add_places_to_neo4j(G, driver)
    add_mesh_places_to_neo4j(G, driver)
    add_rooms_to_neo4j(G, driver)
    add_buildings_to_neo4j(G, driver)

    # find the boxes
    records, summary, keys = driver.execute_query(
        """MATCH (p:Object {class: "box"}) 
        RETURN p.nodeSymbol AS nodeSymbol, p.class AS class, p.center AS center""",
        database_="neo4j",
    )

    # Loop through results and do something with them
    for record in records:
        print(record.data())

    # find the box closest to the origin
    records, summary, keys = driver.execute_query(
        """
        MATCH (p:Object)
        WHERE p.class IN ["box", "sign"]
        WITH p, point.distance(point({x: 0, y: 0, z: 0}), p.center) AS dist
        RETURN p.nodeSymbol AS nodeSymbol, p.class AS class, dist, p.center AS center
        ORDER BY dist ASC
        LIMIT 1
        """,
        database_="neo4j",
    )

    print("Closest box to origin:")
    # Loop through results and do something with them
    for record in records:
        print(record.data())

    # G = spark_dsg.DynamicSceneGraph.load("scene_graph_full_loop_2.json")
    # pdsg = spark_dsg_to_pydsg(G)
    # plot_dsg_objects(pdsg)
    # plt.savefig("pics/objs1.png")
