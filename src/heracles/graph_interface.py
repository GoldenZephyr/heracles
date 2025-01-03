import spark_dsg

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
    d["x"] = attrs.position[0]
    d["y"] = attrs.position[1]
    d["z"] = attrs.position[2]
    d["class"] = node_classes[str(attrs.semantic_label)]
    return d


def insert_objects_to_db(db, objects):

    return db.execute(
        """
    WITH $objects AS objects
    UNWIND objects AS object
    WITH point({x: object.x, y: object.y, z: object.z}) AS p3d, object
    MERGE (:Object {nodeSymbol: object.nodeSymbol, center: p3d, class: object.class})
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
    MERGE (:Place {nodeSymbol: building.nodeSymbol, center: p3d})
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
