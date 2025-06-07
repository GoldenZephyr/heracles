import spark_dsg
import parse

import pdb

# Inserting objects


def add_objects_from_dsg(G, db):
    objects = [
        obj_to_dict(G.metadata["labelspace"], o)
        for o in G.get_layer(spark_dsg.DsgLayers.OBJECTS).nodes
    ]
    insert_objects_to_db(db, objects)


def obj_to_dict(node_classes, obj):
    attrs = obj.attributes
    d = {}
    d["nodeSymbol"] = str(obj.id)
    d["pos_x"] = attrs.position[0]
    d["pos_y"] = attrs.position[1]
    d["pos_z"] = attrs.position[2]
    d["bbox_x"] = attrs.bounding_box.world_P_center[0]
    d["bbox_y"] = attrs.bounding_box.world_P_center[1]
    d["bbox_z"] = attrs.bounding_box.world_P_center[2]
    d["bbox_l"] = attrs.bounding_box.dimensions[0]
    d["bbox_w"] = attrs.bounding_box.dimensions[1]
    d["bbox_h"] = attrs.bounding_box.dimensions[2]
    d["class"] = node_classes[str(attrs.semantic_label)]
    return d


def insert_objects_to_db(db, objects):
    return db.execute(
    """
    WITH $objects AS objects
    UNWIND objects AS object
    WITH point({x: object.pos_x, y: object.pos_y, z: object.pos_z}) AS p3d, point({x: object.bbox_x, y: object.bbox_y, z: object.bbox_z}) AS bb3d, point({x: object.bbox_l, y: object.bbox_w, z: object.bbox_h}) AS bbdim, object
    MERGE (:Object {nodeSymbol: object.nodeSymbol, center: p3d, bbox_center: bb3d, bbox_dim: bbdim, class: object.class})
    """,
        objects=objects,
    )


# Inserting places


def place_to_dict(place):
    attrs = place.attributes
    d = {}
    d["nodeSymbol"] = str(place.id)
    d["x"] = attrs.position[0]
    d["y"] = attrs.position[1]
    d["z"] = attrs.position[2]
    return d


def add_places_from_dsg(G, db):
    places = [place_to_dict(p) for p in G.get_layer(spark_dsg.DsgLayers.PLACES).nodes]
    insert_places_to_db(db, places)


def insert_places_to_db(db, places):
    return db.execute(
        """
    WITH $places AS places
    UNWIND places AS place
    WITH point({x: place.x, y: place.y, z: place.z}) AS p3d, place
    MERGE (:Place {nodeSymbol: place.nodeSymbol, center: p3d})
    """,
        places=places,
    )


# Inserting mesh places


def mesh_place_to_dict(node_classes, mesh_place):
    attrs = mesh_place.attributes
    d = {}
    d["nodeSymbol"] = str(mesh_place.id)
    d["x"] = attrs.position[0]
    d["y"] = attrs.position[1]
    d["z"] = attrs.position[2]
    d["class"] = node_classes[str(attrs.semantic_label)]
    return d


def add_mesh_places_from_dsg(G, db):
    mesh_places = [
        mesh_place_to_dict(G.metadata["labelspace"], p)
        for p in G.get_layer(spark_dsg.DsgLayers.MESH_PLACES).nodes
    ]
    insert_mesh_places_to_db(db, mesh_places)


def insert_mesh_places_to_db(db, mesh_places):
    return db.execute(
        """
    WITH $mesh_places AS places
    UNWIND places AS place
    WITH point({x: place.x, y: place.y, z: place.z}) AS p3d, place
    MERGE (:MeshPlace {nodeSymbol: place.nodeSymbol, center: p3d, class: place.class})
    """,
        mesh_places=mesh_places,
    )


# Inserting Rooms


def add_rooms_from_dsg(G, db):
    if "room_labelspace" in G.metadata:
        labelspace = G.metadata["room_labelspace"]
    else:
        labelspace = {"0": "Unknown"}

    rooms = [
        room_to_dict(labelspace, r)
        for r in G.get_layer(spark_dsg.DsgLayers.ROOMS).nodes
    ]
    insert_rooms_to_db(db, rooms)


def room_to_dict(node_classes, room):
    attrs = room.attributes
    d = {}
    d["nodeSymbol"] = str(room.id)
    d["x"] = attrs.position[0]
    d["y"] = attrs.position[1]
    d["z"] = attrs.position[2]
    d["class"] = node_classes[str(attrs.semantic_label)]
    return d


def insert_rooms_to_db(db, rooms):

    return db.execute(
        """
    WITH $rooms AS rooms
    UNWIND rooms AS room
    WITH point({x: room.x, y: room.y, z: room.z}) AS p3d, room
    MERGE (:Room {nodeSymbol: room.nodeSymbol, center: p3d, class: room.class})
    """,
        rooms=rooms,
    )


# Inserting Buildings


def building_to_dict(building):
    attrs = building.attributes
    d = {}
    d["nodeSymbol"] = str(building.id)
    d["x"] = attrs.position[0]
    d["y"] = attrs.position[1]
    d["z"] = attrs.position[2]
    return d


def add_buildings_from_dsg(G, db):
    buildings = [
        building_to_dict(p) for p in G.get_layer(spark_dsg.DsgLayers.PLACES).nodes
    ]
    insert_buildings_to_db(db, buildings)


def insert_buildings_to_db(db, buildings):
    return db.execute(
        """
    WITH $buildings AS buildings
    UNWIND buildings AS building
    WITH point({x: building.x, y: building.y, z: building.z}) AS p3d, building
    MERGE (:Building {nodeSymbol: building.nodeSymbol, center: p3d})
    """,
        buildings=buildings,
    )


def add_edges_from_dsg(G, db):

    print("Adding Edges")
    layer_id_to_layer_str = G.metadata["LayerIdToLayerStr"]

    object_object_edges = []
    for n in G.get_layer(spark_dsg.DsgLayers.OBJECTS).nodes:
        from_ns = str(n.id)
        for sid in n.siblings():
            to_ns = str(spark_dsg.NodeSymbol(sid))
            object_object_edges.append({"from": from_ns, "to": to_ns})

    insert_edges(db, "OBJECT_CONNECTED", "Object", "Object", object_object_edges)

    print("Finished Object Edges")

    place_place_edges = []
    place_object_edges = []
    for n in G.get_layer(spark_dsg.DsgLayers.PLACES).nodes:
        from_ns = str(n.id)
        for sid in n.siblings():
            to_ns = str(spark_dsg.NodeSymbol(sid))
            place_place_edges.append({"from": from_ns, "to": to_ns})

        for cid in n.children():
            to_ns = str(spark_dsg.NodeSymbol(cid))
            to_layer_id = G.get_node(cid).layer
            to_layer_str = layer_id_to_layer_str[str(to_layer_id)]
            assert (
                to_layer_str == "Object"
            ), "Currently Places can only have Objects as children"
            place_object_edges.append({"from": from_ns, "to": to_ns})

    insert_edges(db, "PLACE_CONNECTED", "Place", "Place", place_place_edges)
    insert_edges(db, "CONTAINS", "Place", "Object", place_object_edges)

    print("Finished Place Edges")

    mp_mp_edges = []
    for n in G.get_layer(spark_dsg.DsgLayers.MESH_PLACES).nodes:
        from_ns = str(n.id)
        for sid in n.siblings():
            to_ns = str(spark_dsg.NodeSymbol(sid))
            mp_mp_edges.append({"from": from_ns, "to": to_ns})

    insert_edges(db, "MESH_PLACE_CONNECTED", "MeshPlace", "MeshPlace", mp_mp_edges)
    print("Finished Mesh Place Edges")

    room_room_edges = []
    room_place_edges = []
    for n in G.get_layer(spark_dsg.DsgLayers.ROOMS).nodes:
        from_ns = str(n.id)
        for sid in n.siblings():
            to_ns = str(spark_dsg.NodeSymbol(sid))
            room_room_edges.append({"from": from_ns, "to": to_ns})
        for cid in n.children():
            to_ns = str(spark_dsg.NodeSymbol(cid))
            to_layer_id = G.get_node(cid).layer
            to_layer_str = layer_id_to_layer_str[str(to_layer_id)]
            assert (
                to_layer_str == "Place"
            ), "Currently Rooms can only have Places as children"
            room_place_edges.append({"from": from_ns, "to": to_ns})

    insert_edges(db, "ROOM_CONNECTED", "Room", "Room", room_room_edges)
    insert_edges(db, "CONTAINS", "Room", "Place", room_place_edges)

    print("Finished Room Edges")

    building_building_edges = []
    building_room_edges = []

    for n in G.get_layer(spark_dsg.DsgLayers.BUILDINGS).nodes:
        from_ns = str(n.id)
        for sid in n.siblings():
            to_ns = str(spark_dsg.NodeSymbol(sid))
            building_building_edges.append({"from": from_ns, "to": to_ns})
        for cid in n.children():
            to_ns = str(spark_dsg.NodeSymbol(cid))
            to_layer_id = G.get_node(cid).layer
            to_layer_str = layer_id_to_layer_str[str(to_layer_id)]
            assert (
                to_layer_str == "Room"
            ), "Currently Buildings can only have Rooms as children"
            building_room_edges.append({"from": from_ns, "to": to_ns})

    insert_edges(
        db, "BUILDING_CONNECTED", "Building", "Building", building_building_edges
    )
    insert_edges(db, "CONTAINS", "Building", "Room", building_room_edges)
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

def get_layer_nodes(db, layer):
    if layer == "Object":
      records, summary, keys = db.execute(
          f"""
          Match (p:{layer})
          RETURN p.nodeSymbol as nodeSymbol, p.class as class, p.center as center, p.bbox_center as bbox_center, p.bbox_dim as bbox_dim
          """
      )
    else:
      records, summary, keys = db.execute(
          f"""
          Match (p:{layer})
          RETURN p.nodeSymbol as nodeSymbol, p.class as class, p.center as center
          """
      )
    return records, summary, keys

def get_db_edges(db, edge_type, from_label, to_label):
    query = f"""
    MATCH (a:{from_label})-[:{edge_type}]->(b:{to_label})
    RETURN a.nodeSymbol as from, b.nodeSymbol as to
    """
    records, summary, keys = db.execute(query)
    return records, summary, keys

def insert_edges_to_spark(G, records):
    for record in records:
        G.insert_edge(str_to_ns_value(record['from']), str_to_ns_value(record['to']))
    return

def str_to_ns_value(s):
    p = parse.parse("{}({})", s)
    key = p.fixed[0]
    idx = int(p.fixed[1])
    ns = spark_dsg.NodeSymbol(key, idx)
    return ns.value

def add_edges_from_db(db, G):
    # Add the MeshPlace-MeshPlace edges
    records, _, _ = get_db_edges(db, 'MESH_PLACE_CONNECTED', 'MeshPlace', 'MeshPlace') 
    insert_edges_to_spark(G, records)
    
    # Add the Object-Object edges
    records, _, _ = get_db_edges(db, 'OBJECT_CONNECTED', 'Object', 'Object') 
    insert_edges_to_spark(G, records)

    # Add the Place-Object edges
    records, _, _ = get_db_edges(db, 'CONTAINS', 'Place', 'Object')
    insert_edges_to_spark(G, records)

    # Add the Place-Place edges
    records, _, _ = get_db_edges(db, 'PLACE_CONNECTED', 'Place', 'Place')
    insert_edges_to_spark(G, records)

    # Add the Room-Place edges
    records, _, _ = get_db_edges(db, 'CONTAINS', 'Room', 'Place')
    insert_edges_to_spark(G, records)

    # Add the Room-Room edges
    records, _, _ = get_db_edges(db, 'ROOM_CONNECTED', 'Room', 'Room')
    insert_edges_to_spark(G, records)

    # Add the Building-Room edges
    records, _, _ = get_db_edges(db, 'CONTAINS', 'Building', 'Room')
    insert_edges_to_spark(G, records)

    # Add the Building-Building edges
    records, _, _ = get_db_edges(db, 'BUILDING_CONNECTED', 'Building', 'Building')
    insert_edges_to_spark(G, records)
    return

def db_to_spark_mesh_place(mp, label_to_semantic_id):
    attrs = spark_dsg.Place2dNodeAttributes()
    attrs.name = mp['nodeSymbol']
    attrs.position = mp['center']
    attrs.semantic_label = label_to_semantic_id[mp['class']]
    return attrs

def db_to_spark_place(p, label_to_semantic_id):
    attrs = spark_dsg.PlaceNodeAttributes()
    attrs.name = p['nodeSymbol']
    attrs.position = p['center']
    return attrs

def db_to_spark_object(o, label_to_semantic_id):
    attrs = spark_dsg.ObjectNodeAttributes()
    attrs.name = o['nodeSymbol']
    attrs.position = o['center']
    attrs.semantic_label = label_to_semantic_id[o['class']]
    attrs.bounding_box = spark_dsg.BoundingBox(
      [o["bbox_dim"][0], o["bbox_dim"][1], o["bbox_dim"][2]], # dimensions
      [o["bbox_center"][0], o["bbox_center"][1], o["bbox_center"][2]], # center
    )
    return attrs

def db_to_spark_room(r, room_label_to_semantic_id):
    attrs = spark_dsg.RoomNodeAttributes()
    attrs.name = r['nodeSymbol']
    attrs.position = r['center']
    attrs.semantic_label = room_label_to_semantic_id[r['class']]
    attrs.bounding_box = spark_dsg.BoundingBox([0.1,0.1,0.1])
    return attrs

def db_to_spark_dsg(db, label_to_semantic_id, room_label_to_semantic_id):
    # Initialize the spark_dsg scene graph object
    layers = [
        spark_dsg.DsgLayers.PLACES,
        spark_dsg.DsgLayers.MESH_PLACES,
        spark_dsg.DsgLayers.OBJECTS,
        spark_dsg.DsgLayers.ROOMS,
        spark_dsg.DsgLayers.BUILDINGS,
    ]
    G = spark_dsg.DynamicSceneGraph(layers)

    # Add the MeshPlaces layer (Placed2d)
    records, summary, keys = get_layer_nodes(db, 'MeshPlace')
    for record in records:
        attrs = db_to_spark_mesh_place(record, label_to_semantic_id)
        G.add_node(
            spark_dsg.DsgLayers.MESH_PLACES,
            str_to_ns_value(record['nodeSymbol']),
            attrs
        )

    # Add the Places layer (Places3d)
    records, summary, keys = get_layer_nodes(db, 'Place')
    for record in records:
        attrs = db_to_spark_place(record, label_to_semantic_id)
        G.add_node(
            spark_dsg.DsgLayers.PLACES,
            str_to_ns_value(record['nodeSymbol']),
            attrs
        )

    # Add the Objects layer
    records, summary, keys = get_layer_nodes(db, 'Object')
    for record in records:
        attrs = db_to_spark_object(record, label_to_semantic_id)
        G.add_node(
            spark_dsg.DsgLayers.OBJECTS,
            str_to_ns_value(record['nodeSymbol']),
            attrs
        )

    # Add the Rooms layer
    records, summary, keys = get_layer_nodes(db, 'Room')
    for record in records:
        attrs = db_to_spark_room(record, room_label_to_semantic_id)
        G.add_node(
            spark_dsg.DsgLayers.ROOMS,
            str_to_ns_value(record['nodeSymbol']),
            attrs
        )

    # Add all of the edges (inter- and intra-layer)
    add_edges_from_db(db, G)
    return G
