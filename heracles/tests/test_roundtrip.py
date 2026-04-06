"""
Round-trip tests: spark_dsg → Neo4j → spark_dsg.

Verifies that loading a DSG into Neo4j via heracles and reconstructing it
preserves node data for both old and new scene graph formats.

These tests require:
- Neo4j running on localhost:7687 with credentials neo4j/neo4j_pw
- The old example DSG at heracles/examples/scene_graphs/example_dsg.json
- Optionally, the new DSG (set NEW_DSG_PATH env var or skip)
"""

import os

import neo4j
import numpy as np
import pytest
import spark_dsg

from heracles import constants
from heracles.graph_interface import (
    db_record_to_spark_attrs,
    db_to_spark_dsg,
    get_layer_nodes,
    initialize_db,
    spark_dsg_to_db,
)
from heracles.query_interface import Neo4jWrapper
from heracles.utils import get_labelspace

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

OLD_DSG_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "examples",
    "scene_graphs",
    "example_dsg.json",
)

NEW_DSG_PATH = os.environ.get(
    "HERACLES_TEST_NEW_DSG",
    os.path.expanduser(
        "~/software/mit/awesome-dcist-t4/scene_graphs/2026-04-02/"
        "2026_04_02_b10_prior_map_2/hydra/backend/dsg_with_mesh.json"
    ),
)

NEO4J_URI = "neo4j://127.0.0.1:7687"
NEO4J_AUTH = ("neo4j", "neo4j_pw")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _try_drop_index(db, index_name):
    try:
        db.execute(f"DROP INDEX {index_name}")
    except neo4j.exceptions.DatabaseError:
        pass


@pytest.fixture()
def db():
    wrapper = Neo4jWrapper(NEO4J_URI, NEO4J_AUTH, atomic_queries=True)
    wrapper.connect()
    yield wrapper
    wrapper.close()


def _load_and_push(db, dsg_path):
    """Load a DSG file, add metadata, push to Neo4j. Returns the original DSG."""
    original = spark_dsg.DynamicSceneGraph.load(dsg_path)
    obj_labels = get_labelspace("ade20k_mit_label_space.yaml")
    room_labels = get_labelspace("b45_label_space.yaml")
    original.metadata.add({"labelspace": obj_labels})
    original.metadata.add({"room_labelspace": room_labels})
    original.metadata.add(
        {
            "LayerIdToHeraclesLayerStr": {
                2: "Object",
                3: "Place",
                4: "Room",
                5: "Building",
                20: "MeshPlace",
                "3[1]": "MeshPlace",
            }
        }
    )
    initialize_db(db)
    spark_dsg_to_db(original, db, source_file_path=dsg_path)
    return original, obj_labels, room_labels


# ---------------------------------------------------------------------------
# Old DSG round-trip tests
# ---------------------------------------------------------------------------


class TestOldDsgRoundtrip:
    def test_node_counts_preserved(self, db):
        """Node counts per layer match after round-trip."""
        original, obj_labels, room_labels = _load_and_push(db, OLD_DSG_PATH)

        for layer_label, layer_enum in [
            (constants.OBJECTS, spark_dsg.DsgLayers.OBJECTS),
            (constants.MESH_PLACES, spark_dsg.DsgLayers.MESH_PLACES),
            (constants.ROOMS, spark_dsg.DsgLayers.ROOMS),
        ]:
            orig_count = original.get_layer(layer_enum).num_nodes()
            db_nodes, _, _ = get_layer_nodes(db, layer_label)
            assert len(db_nodes) == orig_count, (
                f"{layer_label}: expected {orig_count}, got {len(db_nodes)}"
            )

    def test_attr_type_stored(self, db):
        """Every node in Neo4j has an attr_type property."""
        _load_and_push(db, OLD_DSG_PATH)

        for label in [constants.OBJECTS, constants.MESH_PLACES, constants.ROOMS]:
            nodes, _, _ = get_layer_nodes(db, label)
            for node in nodes:
                assert "attr_type" in node, (
                    f"Node {node.get('nodeSymbol')} missing attr_type"
                )

    def test_object_properties_preserved(self, db):
        """Object nodes preserve class, name, position, and bounding box."""
        original, obj_labels, room_labels = _load_and_push(db, OLD_DSG_PATH)

        nodes, _, _ = get_layer_nodes(db, constants.OBJECTS)
        assert len(nodes) > 0

        sample = nodes[0]
        assert "class" in sample
        assert "center" in sample
        assert "bbox_x" in sample
        assert sample["attr_type"] == "ObjectNodeAttributes"
        assert "color_r" in sample
        assert "registered" in sample

    def test_room_properties_preserved(self, db):
        """Room nodes preserve class and position."""
        _load_and_push(db, OLD_DSG_PATH)

        nodes, _, _ = get_layer_nodes(db, constants.ROOMS)
        assert len(nodes) > 0

        sample = nodes[0]
        assert "class" in sample
        assert sample["attr_type"] == "RoomNodeAttributes"

    def test_reconstruction(self, db):
        """Reconstructed DSG has the same node symbols as original."""
        original, obj_labels, room_labels = _load_and_push(db, OLD_DSG_PATH)

        label_to_sid = {v: k for k, v in obj_labels.items()}
        room_to_sid = {v: k for k, v in room_labels.items()}
        layer_map = {2: "Object", 3: "Place", 4: "Room", 5: "Building", 20: "MeshPlace"}

        reconstructed = db_to_spark_dsg(db, layer_map, label_to_sid, room_to_sid)

        # Compare object node symbols.
        orig_objs = {n.id.str(True) for n in original.get_layer(spark_dsg.DsgLayers.OBJECTS).nodes}
        recon_objs = {n.id.str(True) for n in reconstructed.get_layer(spark_dsg.DsgLayers.OBJECTS).nodes}
        assert orig_objs == recon_objs

    def test_source_file_path_stored(self, db):
        """The source file path is stored as _GraphMetadata."""
        _load_and_push(db, OLD_DSG_PATH)

        result = db.query(
            "MATCH (m:_GraphMetadata {key: 'source'}) RETURN m.file_path AS path"
        )
        assert len(result) == 1
        assert result[0]["path"].endswith("example_dsg.json")


# ---------------------------------------------------------------------------
# New DSG round-trip tests (TravNodeAttributes)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not os.path.exists(NEW_DSG_PATH),
    reason=f"New DSG not found at {NEW_DSG_PATH}",
)
class TestNewDsgRoundtrip:
    def test_trav_node_properties(self, db):
        """TravNodeAttributes store radii, states, and timestamps."""
        _load_and_push(db, NEW_DSG_PATH)

        nodes, _, _ = get_layer_nodes(db, constants.MESH_PLACES)
        assert len(nodes) > 0

        sample = nodes[0]
        assert sample["attr_type"] == "TravNodeAttributes"
        assert "radii" in sample
        assert "states" in sample
        assert "min_radius" in sample
        assert "max_radius" in sample
        assert "first_observed_ns" in sample
        assert isinstance(sample["radii"], list)
        assert isinstance(sample["states"], list)

    def test_khronos_object_properties(self, db):
        """KhronosObjectAttributes store bbox, color, timestamps."""
        _load_and_push(db, NEW_DSG_PATH)

        nodes, _, _ = get_layer_nodes(db, constants.OBJECTS)
        assert len(nodes) > 0

        sample = nodes[0]
        assert sample["attr_type"] == "KhronosObjectAttributes"
        assert "bbox_x" in sample
        assert "color_r" in sample
        assert "first_observed_ns" in sample
        # KhronosObjectAttributes has list timestamps
        assert isinstance(sample["first_observed_ns"], list)

    def test_node_counts(self, db):
        """Correct node counts for the new DSG."""
        _load_and_push(db, NEW_DSG_PATH)

        objects, _, _ = get_layer_nodes(db, constants.OBJECTS)
        mesh_places, _, _ = get_layer_nodes(db, constants.MESH_PLACES)
        assert len(objects) == 14
        assert len(mesh_places) == 160


# ---------------------------------------------------------------------------
# Attr type required test
# ---------------------------------------------------------------------------


class TestAttrTypeRequired:
    def test_missing_attr_type_raises(self, db):
        """Reconstruction raises ValueError if attr_type is missing."""
        # Manually insert a node without attr_type.
        db.execute("MATCH (n) DETACH DELETE n")
        db.execute(
            "CREATE (:Object {nodeSymbol: 'test_no_type', "
            "center: point({x: 0, y: 0, z: 0})})"
        )

        records, _, _ = get_layer_nodes(db, constants.OBJECTS)
        assert len(records) == 1

        with pytest.raises(ValueError, match="no attr_type"):
            db_record_to_spark_attrs(records[0], {}, {})
