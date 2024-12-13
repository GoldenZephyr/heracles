import spark_dsg
from pydsg.pydsg_translator import *
from pydsg.dsg_plotter import *
from neo4j import GraphDatabase
import json

URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "neo4jiscool")

def add_object_to_neo4j(node, driver):
    attrs = node.attributes
    param = {
        "nodeSymbol": str(node.id), 
        "x": attrs.position[0], "y": attrs.position[1], "z": attrs.position[2],
        "class": SEMANTIC_ID_TO_LABEL[attrs.semantic_label]
        }
    results = driver.execute_query(
        """
        WITH point({x: $x, y: $y, z: $z}) AS p3d
        MERGE (:Object {nodeSymbol: $nodeSymbol, center: p3d, class: $class})
        """,
        parameters_=param,
        database_="neo4j",
    )

def add_place_to_neo4j(node, driver):
    attrs = node.attributes
    param = {
        "nodeSymbol": str(node.id), 
        "x": attrs.position[0], "y": attrs.position[1], "z": attrs.position[2],
        }
    results = driver.execute_query(
        """
        WITH point({x: $x, y: $y, z: $z}) AS p3d
        MERGE (:Place {nodeSymbol: $nodeSymbol, center: p3d})
        """,
        parameters_=param,
        database_="neo4j",
    )

def add_mesh_places_to_neo4j(node, driver):
    attrs = node.attributes
    param = {
        "nodeSymbol": str(node.id), 
        "x": attrs.position[0], "y": attrs.position[1], "z": attrs.position[2],
        "class": SEMANTIC_ID_TO_LABEL[attrs.semantic_label]
        }
    results = driver.execute_query(
        """
        WITH point({x: $x, y: $y, z: $z}) AS p3d
        MERGE (:MeshPlace {nodeSymbol: $nodeSymbol, center: p3d, class: $class})
        """,
        parameters_=param,
        database_="neo4j",
    )

def add_building_to_neo4j(node, driver):
    attrs = node.attributes
    param = {
        "nodeSymbol": str(node.id), 
        "x": attrs.position[0], "y": attrs.position[1], "z": attrs.position[2]}
    results = driver.execute_query(
        """
        WITH point({x: $x, y: $y, z: $z}) AS p3d
        MERGE (:Building {nodeSymbol: $nodeSymbol, center: p3d})
        """,
        parameters_=param,
        database_="neo4j",
    )

def add_room_to_neo4j(node, driver):
    attrs = node.attributes
    param = {
        "nodeSymbol": str(node.id), 
        "x": attrs.position[0], "y": attrs.position[1], "z": attrs.position[2],
        #"class": SEMANTIC_ID_TO_LABEL[attrs.semantic_label]
        }
    results = driver.execute_query(
        """
        WITH point({x: $x, y: $y, z: $z}) AS p3d
        MERGE (:Room {nodeSymbol: $nodeSymbol, center: p3d})
        """,
        parameters_=param,
        database_="neo4j",
    )

# TODO: write this to graph
layers = {
    spark_dsg.DsgLayers.AGENTS : "agent",
    spark_dsg.DsgLayers.OBJECTS : "object",
    spark_dsg.DsgLayers.BUILDINGS : "building",
    spark_dsg.DsgLayers.MESH_PLACES : "mesh_place",
    spark_dsg.DsgLayers.PLACES : "place",
    spark_dsg.DsgLayers.ROOMS : "room",
    spark_dsg.DsgLayers.STRUCTURE : "structure",
}

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    G = spark_dsg.DynamicSceneGraph.load("scene_graph_full_loop_2.json")

    for node in G.get_layer(spark_dsg.DsgLayers.OBJECTS).nodes:
        add_object_to_neo4j(node, driver)

    for node in G.get_layer(spark_dsg.DsgLayers.PLACES).nodes:
        add_place_to_neo4j(node, driver)

    for node in G.get_layer(spark_dsg.DsgLayers.MESH_PLACES).nodes:
        add_mesh_places_to_neo4j(node, driver)

    for node in G.get_layer(spark_dsg.DsgLayers.ROOMS).nodes:
        add_room_to_neo4j(node, driver)

    for node in G.get_layer(spark_dsg.DsgLayers.BUILDINGS).nodes:
        add_building_to_neo4j(node, driver)


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