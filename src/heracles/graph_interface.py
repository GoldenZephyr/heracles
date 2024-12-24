import spark_dsg


def add_objects_to_neo4j(G, driver):
    for node in G.get_layer(spark_dsg.DsgLayers.OBJECTS).nodes:
        add_object_to_neo4j(node, driver, G.metadata["labelspace"])


def add_places_to_neo4j(G, driver):
    for node in G.get_layer(spark_dsg.DsgLayers.PLACES).nodes:
        add_place_to_neo4j(node, driver)


def add_mesh_places_to_neo4j(G, driver):
    for node in G.get_layer(spark_dsg.DsgLayers.MESH_PLACES).nodes:
        add_mesh_place_to_neo4j(node, driver, G.metadata["labelspace"])


def add_rooms_to_neo4j(G, driver):
    if "room_labelspace" in G.metadata:
        labelspace = G.metadata["room_labelspace"]
    else:
        labelspace = {"0": "Unknown"}
    for node in G.get_layer(spark_dsg.DsgLayers.ROOMS).nodes:
        add_room_to_neo4j(node, driver, labelspace)


def add_buildings_to_neo4j(G, driver):
    for node in G.get_layer(spark_dsg.DsgLayers.BUILDINGS).nodes:
        add_building_to_neo4j(node, driver)


def add_object_to_neo4j(node, driver, node_classes: dict) -> None:
    attrs = node.attributes
    param = {
        "nodeSymbol": str(node.id),
        "x": attrs.position[0],
        "y": attrs.position[1],
        "z": attrs.position[2],
        "class": node_classes[str(attrs.semantic_label)],
    }
    return driver.execute_query(
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
        "x": attrs.position[0],
        "y": attrs.position[1],
        "z": attrs.position[2],
    }
    return driver.execute_query(
        """
        WITH point({x: $x, y: $y, z: $z}) AS p3d
        MERGE (:Place {nodeSymbol: $nodeSymbol, center: p3d})
        """,
        parameters_=param,
        database_="neo4j",
    )


def add_mesh_place_to_neo4j(node, driver, node_classes: dict):
    attrs = node.attributes
    param = {
        "nodeSymbol": str(node.id),
        "x": attrs.position[0],
        "y": attrs.position[1],
        "z": attrs.position[2],
        "class": node_classes[str(attrs.semantic_label)],
    }
    return driver.execute_query(
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
        "x": attrs.position[0],
        "y": attrs.position[1],
        "z": attrs.position[2],
    }
    return driver.execute_query(
        """
        WITH point({x: $x, y: $y, z: $z}) AS p3d
        MERGE (:Building {nodeSymbol: $nodeSymbol, center: p3d})
        """,
        parameters_=param,
        database_="neo4j",
    )


def add_room_to_neo4j(node, driver, node_classes: dict):
    attrs = node.attributes
    param = {
        "nodeSymbol": str(node.id),
        "x": attrs.position[0],
        "y": attrs.position[1],
        "z": attrs.position[2],
        "class": node_classes[str(attrs.semantic_label)],
    }
    return driver.execute_query(
        """
        WITH point({x: $x, y: $y, z: $z}) AS p3d
        MERGE (:Room {nodeSymbol: $nodeSymbol, center: p3d, class: $class})
        """,
        parameters_=param,
        database_="neo4j",
    )


## TODO:


def spark_object_from_neo4j(db_object):
    return None


def spark_place_from_neo4j(db_place):
    return None


def spark_mesh_place_from_neo4j(db_mesh_place):
    return None


def spark_room_from_neo4j(db_room):
    return None


def spark_building_from_neo4j(db_building):
    return None
