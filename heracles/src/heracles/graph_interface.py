"""
Bidirectional conversion between spark_dsg scene graphs and Neo4j.

Design
------
Node conversion uses **property-presence** rather than hardcoded per-type
functions.  ``node_to_dict()`` inspects which attributes a node actually has
(via ``hasattr``) and stores whatever it finds, along with an ``attr_type``
string (the Python class name) so reconstruction can create the right C++
attribute class.

This makes the pipeline robust to new attribute types (e.g.,
TravNodeAttributes in MESH_PLACES, KhronosObjectAttributes in OBJECTS)
without code changes.

Lossy properties
----------------
Some attribute data is NOT round-tripped through Neo4j:
- TravNodeAttributes boundary (radii, states) — deferred, store only position
- Place2dNodeAttributes boundary (polygon points) — deferred
- AgentNodeAttributes (world_R_body, dbow) — not stored

The source JSON file should be considered the authoritative copy.  Neo4j
stores the subset needed for visualization and editing.

Edge handling remains layer-aware since edge types are determined by the
layer relationship (intralayer vs interlayer), not by attribute types.
"""

import logging

import neo4j
import parse
import spark_dsg

from . import constants

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Attribute type registry for reconstruction (Neo4j → spark_dsg)
# ---------------------------------------------------------------------------

# Maps attr_type strings (stored on Neo4j nodes) to spark_dsg attribute
# constructors.  Used by db_record_to_spark_attrs() to create the right
# C++ class when reconstructing a DSG from the database.
#
# Lives here (not in constants.py) because it maps to C++ classes and is
# only used during reconstruction — it's conversion logic, not a shared
# constant.
ATTR_TYPE_REGISTRY = {
    "ObjectNodeAttributes": spark_dsg.ObjectNodeAttributes,
    "PlaceNodeAttributes": spark_dsg.PlaceNodeAttributes,
    "Place2dNodeAttributes": spark_dsg.Place2dNodeAttributes,
    "RoomNodeAttributes": spark_dsg.RoomNodeAttributes,
    "NodeAttributes": spark_dsg.NodeAttributes,
}

# Conditionally register types that may not exist in older spark_dsg versions.
if hasattr(spark_dsg, "KhronosObjectAttributes"):
    ATTR_TYPE_REGISTRY["KhronosObjectAttributes"] = spark_dsg.KhronosObjectAttributes
if hasattr(spark_dsg, "TraversabilityNodeAttributes"):
    ATTR_TYPE_REGISTRY["TraversabilityNodeAttributes"] = (
        spark_dsg.TraversabilityNodeAttributes
    )
if hasattr(spark_dsg, "TravNodeAttributes"):
    ATTR_TYPE_REGISTRY["TravNodeAttributes"] = spark_dsg.TravNodeAttributes


# ---------------------------------------------------------------------------
# Database initialization
# ---------------------------------------------------------------------------


def initialize_db(db):
    def try_drop_index(db, index_name):
        try:
            db.execute(f"DROP INDEX {index_name}")
        except neo4j.exceptions.DatabaseError:
            print(f"No index `{index_name}`")

    db.execute("MATCH (n) DETACH DELETE n")

    try_drop_index(db, "object_node_symbol")
    try_drop_index(db, "place_node_symbol")
    try_drop_index(db, "mesh_place_node_symbol")
    try_drop_index(db, "room_node_symbol")
    try_drop_index(db, "building_node_symbol")

    db.execute(
        f"CREATE INDEX object_node_symbol FOR (n:{constants.OBJECTS}) ON (n.nodeSymbol)"
    )
    db.execute(
        f"CREATE INDEX place_node_symbol FOR (n:{constants.PLACES}) ON (n.nodeSymbol)"
    )
    db.execute(
        f"CREATE INDEX mesh_place_node_symbol FOR (n:{constants.MESH_PLACES}) ON (n.nodeSymbol)"
    )
    db.execute(
        f"CREATE INDEX room_node_symbol FOR (n:{constants.ROOMS}) ON (n.nodeSymbol)"
    )
    db.execute(
        f"CREATE INDEX building_node_symbol FOR (n:{constants.BUILDINGS}) ON (n.nodeSymbol)"
    )


# ---------------------------------------------------------------------------
# Generic node → dict conversion (spark_dsg → flat dict for Neo4j)
# ---------------------------------------------------------------------------


def node_to_dict(node, object_labelspace=None, room_labelspace=None):
    """Convert any spark_dsg node to a flat dict for Neo4j storage.

    Uses property presence — works with any attribute type without
    hardcoding.  Stores ``attr_type`` so reconstruction can create the
    right C++ class.  Warns if a semantic_label is not found in the
    labelspace.

    Note: some attribute data (boundary details, agent poses, mesh
    connections) is intentionally NOT stored.  See module docstring.
    """
    attrs = node.attributes
    d = {
        "nodeSymbol": node.id.str(True),
        "attr_type": type(attrs).__name__,
        "pos_x": float(attrs.position[0]),
        "pos_y": float(attrs.position[1]),
        "pos_z": float(attrs.position[2]),
    }

    # SemanticNodeAttributes fields (optional).
    if hasattr(attrs, "semantic_label"):
        ls = (
            room_labelspace
            if isinstance(attrs, spark_dsg.RoomNodeAttributes)
            else object_labelspace
        )
        label_key = str(attrs.semantic_label)
        if ls and label_key in ls:
            d["class"] = ls[label_key]
        else:
            logger.warning(
                "Node %s: semantic_label %s not in labelspace",
                d["nodeSymbol"],
                label_key,
            )

    if hasattr(attrs, "name") and attrs.name:
        d["name"] = attrs.name

    # Color (SemanticNodeAttributes — 3 uint8 RGB values).
    if hasattr(attrs, "color"):
        try:
            c = attrs.color
            d["color_r"] = int(c[0])
            d["color_g"] = int(c[1])
            d["color_b"] = int(c[2])
        except Exception:
            pass

    # Bounding box (ObjectNodeAttributes, RoomNodeAttributes).
    if hasattr(attrs, "bounding_box"):
        try:
            if attrs.bounding_box.is_valid():
                bb = attrs.bounding_box
                d["bbox_x"] = float(bb.world_P_center[0])
                d["bbox_y"] = float(bb.world_P_center[1])
                d["bbox_z"] = float(bb.world_P_center[2])
                d["bbox_l"] = float(bb.dimensions[0])
                d["bbox_w"] = float(bb.dimensions[1])
                d["bbox_h"] = float(bb.dimensions[2])
        except Exception as e:
            logger.warning(
                "Node %s: failed to read bounding_box: %s",
                d["nodeSymbol"],
                e,
            )

    # Registered flag (ObjectNodeAttributes).
    if hasattr(attrs, "registered"):
        d["registered"] = bool(attrs.registered)

    # Base attributes (all nodes have these).
    d["is_active"] = bool(attrs.is_active)
    d["is_predicted"] = bool(attrs.is_predicted)

    # Distance to nearest obstacle (PlaceNodeAttributes).
    if hasattr(attrs, "distance"):
        d["distance"] = float(attrs.distance)

    # Observation timestamps (Traversability, TravNode have single int;
    # KhronosObjectAttributes has lists of ints — store as-is either way).
    if hasattr(attrs, "first_observed_ns"):
        val = attrs.first_observed_ns
        if isinstance(val, (list, tuple)):
            d["first_observed_ns"] = [int(v) for v in val]
        else:
            d["first_observed_ns"] = int(val)
    if hasattr(attrs, "last_observed_ns"):
        val = attrs.last_observed_ns
        if isinstance(val, (list, tuple)):
            d["last_observed_ns"] = [int(v) for v in val]
        else:
            d["last_observed_ns"] = int(val)

    # Room class probabilities — stored as parallel key/value lists
    # since Neo4j doesn't support map properties.
    if hasattr(attrs, "semantic_class_probabilities"):
        probs = attrs.semantic_class_probabilities
        if probs:
            d["class_prob_keys"] = list(probs.keys())
            d["class_prob_values"] = [float(v) for v in probs.values()]

    # TravNodeAttributes boundary (radii + states as flat lists).
    # States are TraversabilityState enums stored as ints.
    if hasattr(attrs, "radii"):
        d["radii"] = [float(r) for r in attrs.radii]
        d["states"] = [int(s) for s in attrs.states]
        d["min_radius"] = float(attrs.min_radius)
        d["max_radius"] = float(attrs.max_radius)

    # Place2dNodeAttributes boundary (polygon of 3D points).
    # Stored as temporary flat lists (boundary_x/y/z), then converted
    # to a native Neo4j Point3D list by insert_nodes_to_db.
    if hasattr(attrs, "boundary") and isinstance(attrs.boundary, list):
        boundary = attrs.boundary
        if boundary:
            d["boundary_x"] = [float(pt[0]) for pt in boundary]
            d["boundary_y"] = [float(pt[1]) for pt in boundary]
            d["boundary_z"] = [float(pt[2]) for pt in boundary]

    return d


# ---------------------------------------------------------------------------
# Generic bulk insert (flat dicts → Neo4j nodes)
# ---------------------------------------------------------------------------


def insert_nodes_to_db(db, layer_label, node_dicts):
    """Bulk insert nodes into Neo4j for a given layer.

    Uses MERGE on nodeSymbol (idempotent).  Position is stored as a Neo4j
    Point3D.  All other dict keys are set as scalar properties via ``n += node``.

    If any nodes have boundary_x/y/z (Place2d polygon points), a follow-up
    query converts them to a native Neo4j Point3D list for spatial queries.
    """
    if not node_dicts:
        logger.info("insert_nodes_to_db: no nodes to insert for layer %s", layer_label)
        return
    db.execute(
        f"""
        WITH $nodes AS nodes
        UNWIND nodes AS node
        WITH point({{x: node.pos_x, y: node.pos_y, z: node.pos_z}}) AS p3d, node
        MERGE (n:{layer_label} {{nodeSymbol: node.nodeSymbol}})
        SET n.center = p3d, n += node
        """,
        nodes=node_dicts,
    )

    # Convert flat boundary_x/y/z lists to native Point3D list.
    # This enables Neo4j spatial functions (point.distance, point.withinBBox)
    # on boundary points in future queries.
    has_boundary = any("boundary_x" in d for d in node_dicts)
    if has_boundary:
        db.execute(
            f"""
            MATCH (n:{layer_label})
            WHERE n.boundary_x IS NOT NULL
            WITH n, range(0, size(n.boundary_x)-1) AS indices
            SET n.boundary = [i IN indices |
                point({{x: n.boundary_x[i], y: n.boundary_y[i], z: n.boundary_z[i]}})]
            REMOVE n.boundary_x, n.boundary_y, n.boundary_z
            """
        )


# ---------------------------------------------------------------------------
# Per-layer wrappers (know which spark_dsg layer to iterate)
# ---------------------------------------------------------------------------


def add_objects_from_dsg(G, db):
    object_labelspace = G.metadata.get().get("labelspace", {})
    nodes = [
        node_to_dict(o, object_labelspace=object_labelspace)
        for o in G.get_layer(spark_dsg.DsgLayers.OBJECTS).nodes
    ]
    insert_nodes_to_db(db, constants.OBJECTS, nodes)


def add_places_from_dsg(G, db):
    nodes = [
        node_to_dict(p) for p in G.get_layer(spark_dsg.DsgLayers.PLACES).nodes
    ]
    insert_nodes_to_db(db, constants.PLACES, nodes)


def add_mesh_places_from_dsg(G, db):
    try:
        mesh_place_layer = G.get_layer(spark_dsg.DsgLayers.MESH_PLACES)
    except IndexError:
        mesh_place_layer = G.get_layer(20)

    object_labelspace = G.metadata.get().get("labelspace", {})
    nodes = [
        node_to_dict(p, object_labelspace=object_labelspace)
        for p in mesh_place_layer.nodes
    ]
    insert_nodes_to_db(db, constants.MESH_PLACES, nodes)


def add_rooms_from_dsg(G, db):
    room_labelspace = G.metadata.get().get("room_labelspace", {"0": "Unknown"})
    nodes = [
        node_to_dict(r, room_labelspace=room_labelspace)
        for r in G.get_layer(spark_dsg.DsgLayers.ROOMS).nodes
    ]
    insert_nodes_to_db(db, constants.ROOMS, nodes)


def add_buildings_from_dsg(G, db):
    nodes = [
        node_to_dict(b) for b in G.get_layer(spark_dsg.DsgLayers.BUILDINGS).nodes
    ]
    insert_nodes_to_db(db, constants.BUILDINGS, nodes)


# ---------------------------------------------------------------------------
# Top-level load: spark_dsg → Neo4j
# ---------------------------------------------------------------------------


def spark_dsg_to_db(G, db, source_file_path=None):
    """Load all nodes and edges from a spark_dsg graph into Neo4j.

    If ``source_file_path`` is provided, it is stored as a ``_GraphMetadata``
    node so that downstream tools (e.g., SGET) can locate the original file
    on disk for mesh data and other large assets not stored in Neo4j.
    """
    add_objects_from_dsg(G, db)
    add_places_from_dsg(G, db)
    add_mesh_places_from_dsg(G, db)
    add_rooms_from_dsg(G, db)
    add_buildings_from_dsg(G, db)
    add_edges_from_dsg(G, db)

    if source_file_path is not None:
        import os

        abs_path = os.path.abspath(source_file_path)
        db.execute(
            "MERGE (m:_GraphMetadata {key: 'source'}) SET m.file_path = $path",
            path=abs_path,
        )


# ---------------------------------------------------------------------------
# Edge insertion (unchanged — layer-aware, not type-aware)
# ---------------------------------------------------------------------------


def add_edges_from_dsg(G, db):
    print("Adding Edges")
    layer_id_to_layer_str = G.metadata.get()["LayerIdToHeraclesLayerStr"]

    object_object_edges = []
    for n in G.get_layer(spark_dsg.DsgLayers.OBJECTS).nodes:
        from_ns = n.id.str(True)
        for sid in n.siblings():
            to_ns = spark_dsg.NodeSymbol(sid).str(True)
            object_object_edges.append({"from": from_ns, "to": to_ns})

    insert_edges(
        db,
        "OBJECT_CONNECTED",
        constants.OBJECTS,
        constants.OBJECTS,
        object_object_edges,
    )

    print("Finished Object Edges")

    place_place_edges = []
    place_object_edges = []
    for n in G.get_layer(spark_dsg.DsgLayers.PLACES).nodes:
        from_ns = n.id.str(True)
        for sid in n.siblings():
            to_ns = spark_dsg.NodeSymbol(sid).str(True)
            place_place_edges.append({"from": from_ns, "to": to_ns})

        for cid in n.children():
            to_ns = spark_dsg.NodeSymbol(cid).str(True)
            to_layer_id = G.get_node(cid).layer.layer
            to_layer_str = layer_id_to_layer_str[str(to_layer_id)]
            assert to_layer_str == constants.OBJECTS, (
                "Currently Places can only have Objects as children"
            )
            place_object_edges.append({"from": from_ns, "to": to_ns})

    insert_edges(
        db, "PLACE_CONNECTED", constants.PLACES, constants.PLACES, place_place_edges
    )
    insert_edges(
        db, "CONTAINS", constants.PLACES, constants.OBJECTS, place_object_edges
    )

    print("Finished Place Edges")

    mp_mp_edges = []
    mesh_place_object_edges = []
    try:
        mesh_place_layer = G.get_layer(spark_dsg.DsgLayers.MESH_PLACES)
    except IndexError:
        mesh_place_layer = G.get_layer(20)
    for n in mesh_place_layer.nodes:
        from_ns = n.id.str(True)
        for sid in n.siblings():
            to_ns = spark_dsg.NodeSymbol(sid).str(True)
            mp_mp_edges.append({"from": from_ns, "to": to_ns})
        for cid in n.children():
            to_ns = spark_dsg.NodeSymbol(cid).str(True)
            to_layer_id = G.get_node(cid).layer.layer
            to_layer_str = layer_id_to_layer_str[str(to_layer_id)]
            assert to_layer_str == constants.OBJECTS, (
                "Currently MeshPlaces can only have Objects as children"
            )
            mesh_place_object_edges.append({"from": from_ns, "to": to_ns})

    insert_edges(
        db,
        "MESH_PLACE_CONNECTED",
        constants.MESH_PLACES,
        constants.MESH_PLACES,
        mp_mp_edges,
    )
    insert_edges(
        db,
        "CONTAINS",
        constants.MESH_PLACES,
        constants.OBJECTS,
        mesh_place_object_edges,
    )
    print("Finished Mesh Place Edges")

    room_room_edges = []
    room_place_edges = []
    room_mesh_place_edges = []
    for n in G.get_layer(spark_dsg.DsgLayers.ROOMS).nodes:
        from_ns = n.id.str(True)
        for sid in n.siblings():
            to_ns = spark_dsg.NodeSymbol(sid).str(True)
            room_room_edges.append({"from": from_ns, "to": to_ns})
        for cid in n.children():
            to_ns = spark_dsg.NodeSymbol(cid).str(True)
            to_layer_id = G.get_node(cid).layer
            to_layer_str = layer_id_to_layer_str[str(to_layer_id)]
            assert to_layer_str in [
                constants.PLACES,
                constants.MESH_PLACES,
            ], "Currently Rooms can only have Places or MeshPlaces as children"
            if to_layer_str == constants.PLACES:
                room_place_edges.append({"from": from_ns, "to": to_ns})
            elif to_layer_str == constants.MESH_PLACES:
                room_mesh_place_edges.append({"from": from_ns, "to": to_ns})

    insert_edges(
        db, "ROOM_CONNECTED", constants.ROOMS, constants.ROOMS, room_room_edges
    )
    insert_edges(db, "CONTAINS", constants.ROOMS, constants.PLACES, room_place_edges)
    insert_edges(
        db, "CONTAINS", constants.ROOMS, constants.MESH_PLACES, room_mesh_place_edges
    )

    print("Finished Room Edges")

    building_building_edges = []
    building_room_edges = []

    for n in G.get_layer(spark_dsg.DsgLayers.BUILDINGS).nodes:
        from_ns = n.id.str(True)
        for sid in n.siblings():
            to_ns = spark_dsg.NodeSymbol(sid).str(True)
            building_building_edges.append({"from": from_ns, "to": to_ns})
        for cid in n.children():
            to_ns = spark_dsg.NodeSymbol(cid).str(True)
            to_layer_id = G.get_node(cid).layer
            to_layer_str = layer_id_to_layer_str[str(to_layer_id)]
            assert to_layer_str == constants.ROOMS, (
                "Currently Buildings can only have Rooms as children"
            )
            building_room_edges.append({"from": from_ns, "to": to_ns})

    insert_edges(
        db,
        "BUILDING_CONNECTED",
        constants.BUILDINGS,
        constants.BUILDINGS,
        building_building_edges,
    )
    insert_edges(
        db, "CONTAINS", constants.BUILDINGS, constants.ROOMS, building_room_edges
    )
    print("Finished Building Edges")


def insert_edges(db, edge_type, from_label, to_label, connections):
    query = f"""
    WITH $connections AS connections
    UNWIND connections AS connection
    MATCH (n1: {from_label} {{nodeSymbol: connection.from}})
    MATCH (n2: {to_label} {{nodeSymbol: connection.to}})
    CREATE (n1)-[:{edge_type}]->(n2)
    """
    ret = db.execute(query, connections=connections)
    return ret


# ---------------------------------------------------------------------------
# Generic node read (Neo4j → flat dict)
# ---------------------------------------------------------------------------


def get_layer_nodes(db, layer):
    """Fetch all nodes for a layer, returning all properties generically."""
    records, summary, keys = db.execute(
        f"""
        MATCH (p:{layer})
        RETURN properties(p) AS props
        """
    )
    return [dict(r["props"]) for r in records], summary, keys


def get_db_edges(db, edge_type, from_label, to_label):
    query = f"""
    MATCH (a:{from_label})-[:{edge_type}]->(b:{to_label})
    RETURN a.nodeSymbol as from, b.nodeSymbol as to
    """
    records, summary, keys = db.execute(query)
    return records, summary, keys


# ---------------------------------------------------------------------------
# Generic reconstruction (Neo4j → spark_dsg)
# ---------------------------------------------------------------------------


def db_record_to_spark_attrs(record, object_labelspace, room_labelspace):
    """Create a spark_dsg attribute object from a Neo4j property dict.

    Uses ``attr_type`` (stored on the node) to pick the right C++ class.
    Raises ValueError if attr_type is missing — the database must be
    reloaded from the source JSON to populate it.
    """
    attr_type = record.get("attr_type")
    if attr_type is None:
        raise ValueError(
            f"Node {record.get('nodeSymbol', '?')} has no attr_type property. "
            f"This database may have been populated by an older version of heracles. "
            f"Please reload the source JSON file to update the database."
        )
    cls = ATTR_TYPE_REGISTRY.get(attr_type, spark_dsg.NodeAttributes)
    attrs = cls()

    # Position (all nodes).
    if "center" in record:
        attrs.position = record["center"]

    # Name (SemanticNodeAttributes).
    if hasattr(attrs, "name") and "name" in record:
        attrs.name = record["name"]

    # Semantic label → class (SemanticNodeAttributes).
    if hasattr(attrs, "semantic_label") and "class" in record:
        ls = (
            room_labelspace
            if isinstance(attrs, spark_dsg.RoomNodeAttributes)
            else object_labelspace
        )
        if record["class"] in ls:
            attrs.semantic_label = ls[record["class"]]

    # Color (SemanticNodeAttributes).
    if hasattr(attrs, "color") and "color_r" in record:
        import numpy as np

        attrs.color = np.array(
            [record["color_r"], record["color_g"], record["color_b"]], dtype=np.uint8
        )

    # Bounding box (ObjectNodeAttributes).
    if hasattr(attrs, "bounding_box") and "bbox_dim" in record:
        attrs.bounding_box = spark_dsg.BoundingBox(
            [record["bbox_dim"][0], record["bbox_dim"][1], record["bbox_dim"][2]],
            [
                record["bbox_center"][0],
                record["bbox_center"][1],
                record["bbox_center"][2],
            ],
        )

    # Registered flag (ObjectNodeAttributes).
    if hasattr(attrs, "registered") and "registered" in record:
        attrs.registered = bool(record["registered"])

    # Distance (PlaceNodeAttributes).
    if hasattr(attrs, "distance") and "distance" in record:
        attrs.distance = float(record["distance"])

    # Observation timestamps.
    if hasattr(attrs, "first_observed_ns") and "first_observed_ns" in record:
        attrs.first_observed_ns = int(record["first_observed_ns"])
    if hasattr(attrs, "last_observed_ns") and "last_observed_ns" in record:
        attrs.last_observed_ns = int(record["last_observed_ns"])

    # TravNodeAttributes boundary.
    if hasattr(attrs, "radii") and "radii" in record:
        attrs.radii = [float(r) for r in record["radii"]]
        if "states" in record:
            attrs.states = [int(s) for s in record["states"]]
        if "min_radius" in record:
            attrs.min_radius = float(record["min_radius"])
        if "max_radius" in record:
            attrs.max_radius = float(record["max_radius"])

    # Place2dNodeAttributes boundary (stored as Point3D list in Neo4j).
    if hasattr(attrs, "boundary") and isinstance(getattr(attrs, "boundary", None), list):
        if "boundary" in record and isinstance(record["boundary"], list):
            import numpy as np

            attrs.boundary = [
                np.array([float(pt[0]), float(pt[1]), float(pt[2])])
                for pt in record["boundary"]
            ]

    return attrs




def insert_edges_to_spark(G, records):
    for record in records:
        G.insert_edge(str_to_ns_value(record["from"]), str_to_ns_value(record["to"]))
    return


def str_to_ns_value(string):
    p = parse.parse("{:l}({:d})", string)  # idx in parenthesis
    if not p:
        p = parse.parse("{:l}{:d}", string)  # idx not in parenthesis
    if not p:
        raise ValueError(f"Unexpected Node ID: {string}")

    key = p.fixed[0]
    idx = int(p.fixed[1])
    ns = spark_dsg.NodeSymbol(key, idx)
    return ns.value


def add_edges_from_db(db, G):
    #### INTRALAYER EDGES
    records, _, _ = get_db_edges(
        db, "OBJECT_CONNECTED", constants.OBJECTS, constants.OBJECTS
    )
    insert_edges_to_spark(G, records)
    records, _, _ = get_db_edges(
        db, "MESH_PLACE_CONNECTED", constants.MESH_PLACES, constants.MESH_PLACES
    )
    insert_edges_to_spark(G, records)
    records, _, _ = get_db_edges(
        db, "PLACE_CONNECTED", constants.PLACES, constants.PLACES
    )
    insert_edges_to_spark(G, records)
    records, _, _ = get_db_edges(db, "ROOM_CONNECTED", constants.ROOMS, constants.ROOMS)
    insert_edges_to_spark(G, records)
    records, _, _ = get_db_edges(
        db, "BUILDING_CONNECTED", constants.BUILDINGS, constants.BUILDINGS
    )
    insert_edges_to_spark(G, records)

    #### INTERLAYER EDGES
    records, _, _ = get_db_edges(
        db, "CONTAINS", constants.MESH_PLACES, constants.OBJECTS
    )
    insert_edges_to_spark(G, records)
    records, _, _ = get_db_edges(db, "CONTAINS", constants.PLACES, constants.OBJECTS)
    insert_edges_to_spark(G, records)
    records, _, _ = get_db_edges(db, "CONTAINS", constants.ROOMS, constants.PLACES)
    insert_edges_to_spark(G, records)
    records, _, _ = get_db_edges(db, "CONTAINS", constants.ROOMS, constants.MESH_PLACES)
    insert_edges_to_spark(G, records)
    records, _, _ = get_db_edges(db, "CONTAINS", constants.BUILDINGS, constants.ROOMS)
    insert_edges_to_spark(G, records)
    return


def db_to_spark_dsg(
    db, spark_layer_id_to_layer_name, label_to_semantic_id, room_label_to_semantic_id
):
    """Reconstruct a spark_dsg DynamicSceneGraph from Neo4j.

    Uses ``attr_type`` stored on each node to create the correct attribute
    class.  Falls back to property-presence inference for old data.
    """
    new_scene_graph = spark_dsg.DynamicSceneGraph()
    new_scene_graph.clear(True)

    object_labelspace = spark_dsg.Labelspace(
        {v: k for k, v in label_to_semantic_id.items()}
    )
    new_scene_graph.set_labelspace(object_labelspace, 2, 0)

    for spark_layer_id, heracles_layer_name in spark_layer_id_to_layer_name.items():
        if spark_layer_id == 20:
            new_scene_graph.add_layer(
                3,
                1,
                constants.HERACLES_TO_SPARK_LAYER_NAMES[heracles_layer_name],
            )
        else:
            new_scene_graph.add_layer(
                spark_layer_id,
                0,
                constants.HERACLES_TO_SPARK_LAYER_NAMES[heracles_layer_name],
            )

        records, summary, keys = get_layer_nodes(db, heracles_layer_name)

        for record in records:
            attrs = db_record_to_spark_attrs(
                record, label_to_semantic_id, room_label_to_semantic_id
            )
            new_scene_graph.add_node(
                constants.HERACLES_TO_SPARK_LAYER_NAMES[heracles_layer_name],
                str_to_ns_value(record["nodeSymbol"]),
                attrs,
            )

    add_edges_from_db(db, new_scene_graph)
    return new_scene_graph
