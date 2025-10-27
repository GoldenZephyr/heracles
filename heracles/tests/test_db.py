from importlib.resources import as_file, files

import neo4j
import numpy as np
import pytest
import spark_dsg
import yaml

import heracles
import heracles.resources
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


def add_dsg_metadata(G):
    with as_file(
        files(heracles.resources).joinpath("ade20k_mit_label_space.yaml")
    ) as path:
        with open(str(path), "r") as fo:
            labelspace = yaml.safe_load(fo)
    id_to_label = {item["label"]: item["name"] for item in labelspace["label_names"]}
    G.metadata.add({"labelspace": id_to_label})

    layers = {
        2: "Object",
        5: "Building",
        20: "MeshPlace",
        3: "Place",
        4: "Room",
    }
    G.metadata.add({"LayerIdToHeraclesLayerStr": layers})


def build_test_dsg():
    G = spark_dsg.DynamicSceneGraph()
    G.add_layer(2, "a", spark_dsg.DsgLayers.AGENTS)
    G.add_layer(3, "p", spark_dsg.DsgLayers.PLACES)
    G.add_layer(4, "R", spark_dsg.DsgLayers.ROOMS)
    G.add_layer(5, "B", spark_dsg.DsgLayers.BUILDINGS)
    G.add_layer(20, "P", spark_dsg.DsgLayers.MESH_PLACES)

    room = spark_dsg.RoomNodeAttributes()
    room.position = np.array([0, 0, 0])
    room.semantic_label = 0

    G.add_node(spark_dsg.DsgLayers.ROOMS, spark_dsg.NodeSymbol("R", 0).value, room)

    place1 = spark_dsg.PlaceNodeAttributes()
    place1.position = np.array([-1, 0, 0])
    G.add_node(spark_dsg.DsgLayers.PLACES, spark_dsg.NodeSymbol("p", 0).value, place1)
    place2 = spark_dsg.PlaceNodeAttributes()
    place2.position = np.array([1, 0, 0])
    G.add_node(spark_dsg.DsgLayers.PLACES, spark_dsg.NodeSymbol("p", 1).value, place2)

    place1_2d = spark_dsg.PlaceNodeAttributes()
    place1_2d.position = np.array([-1.1, 0, 0])
    place1_2d.semantic_label = 4  # ground
    G.add_node(
        spark_dsg.DsgLayers.MESH_PLACES, spark_dsg.NodeSymbol("P", 0).value, place1_2d
    )
    place2_2d = spark_dsg.PlaceNodeAttributes()
    place2_2d.position = np.array([1.1, 0, 0])
    place2_2d.semantic_label = 4  # ground
    G.add_node(
        spark_dsg.DsgLayers.MESH_PLACES, spark_dsg.NodeSymbol("P", 1).value, place2_2d
    )

    obj1 = spark_dsg.ObjectNodeAttributes()
    obj1.position = np.array([-1.5, 0, 0])
    obj1.semantic_label = 34  # box
    G.add_node(spark_dsg.DsgLayers.OBJECTS, spark_dsg.NodeSymbol("o", 0).value, obj1)
    obj2 = spark_dsg.PlaceNodeAttributes()
    obj2.position = np.array([1.5, 0, 0])
    obj2.semantic_label = 15  # rock
    G.add_node(spark_dsg.DsgLayers.OBJECTS, spark_dsg.NodeSymbol("o", 1).value, obj2)

    G.insert_edge(
        spark_dsg.NodeSymbol("R", 0).value, spark_dsg.NodeSymbol("p", 0).value
    )
    G.insert_edge(
        spark_dsg.NodeSymbol("R", 0).value, spark_dsg.NodeSymbol("p", 1).value
    )
    G.insert_edge(
        spark_dsg.NodeSymbol("p", 0).value, spark_dsg.NodeSymbol("p", 1).value
    )
    G.insert_edge(
        spark_dsg.NodeSymbol("p", 0).value, spark_dsg.NodeSymbol("o", 0).value
    )
    G.insert_edge(
        spark_dsg.NodeSymbol("p", 1).value, spark_dsg.NodeSymbol("o", 1).value
    )
    G.insert_edge(
        spark_dsg.NodeSymbol("P", 0).value, spark_dsg.NodeSymbol("P", 1).value
    )

    return G


@pytest.fixture(scope="module")
def populated_db():
    G = build_test_dsg()
    add_dsg_metadata(G)

    # IP / Port for database
    URI = "neo4j://127.0.0.1:7687"
    # Database name / password for database
    AUTH = ("neo4j", "neo4j_pw")
    db = Neo4jWrapper(URI, AUTH, atomic_queries=True, print_profiles=False)
    db.connect()

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

    add_objects_from_dsg(G, db)
    add_places_from_dsg(G, db)
    add_mesh_places_from_dsg(G, db)
    add_rooms_from_dsg(G, db)
    add_buildings_from_dsg(G, db)
    add_edges_from_dsg(G, db)

    yield db

    db.close()


def test_rooms(populated_db):
    q = populated_db.query(
        """MATCH (r: Room {nodeSymbol: "R0"}) RETURN r.nodeSymbol as ns, r.center as center"""
    )
    assert len(q) == 1
    assert q[0]["ns"] == "R0"
    assert np.all(np.isclose(q[0]["center"], np.array([0, 0, 0])))


def test_places(populated_db):
    q = populated_db.query("""MATCH (p: Place {nodeSymbol: "p0"}) RETURN p""")
    assert np.all(np.isclose(np.array([-1, 0, 0]), q[0]["p"]["center"]))

    q = populated_db.query("""MATCH (p: Place) RETURN p""")
    assert len(q) == 2


def test_mesh_places(populated_db):
    q = populated_db.query("""MATCH (p: MeshPlace) RETURN p""")
    assert len(q) == 2

    q = populated_db.query("""MATCH (p: MeshPlace {nodeSymbol: "P0"}) RETURN p""")
    assert np.all(np.isclose(np.array([-1.1, 0, 0]), q[0]["p"]["center"]))
    assert q[0]["p"]["class"] == "ground"


def test_objects(populated_db):
    q = populated_db.query("""MATCH (o: Object) RETURN o""")
    assert len(q) == 2

    q = populated_db.query("""MATCH (o: Object {class: "box"}) RETURN o""")
    assert len(q) == 1
    assert q[0]["o"]["nodeSymbol"] == "o0"


def test_edges(populated_db):
    q = populated_db.query(
        """MATCH (r: Room {nodeSymbol: "R0"})-[:CONTAINS*]->(o: Object) RETURN o"""
    )

    assert len(q) == 2
    assert q[0]["o"]["class"] in ["box", "rock"]
    assert q[1]["o"]["class"] in ["box", "rock"]
