import os
from importlib.resources import as_file, files

import yaml

import heracles
import heracles.resources
from heracles.graph_interface import (
    initialize_db,
    spark_dsg_to_db,
)
from heracles.query_interface import Neo4jWrapper


def get_labelspace(labelspace_name):
    if os.path.exists(labelspace_name):
        with open(labelspace_name, "r") as file:
            labelspace = yaml.safe_load(file)
    else:
        with as_file(files(heracles.resources).joinpath(labelspace_name)) as path:
            with open(str(path), "r") as file:
                labelspace = yaml.safe_load(file)
    id_to_label = {item["label"]: item["name"] for item in labelspace["label_names"]}
    return id_to_label


def extract_labelspaces_from_dsg(G):
    """Extract object and room labelspaces from DSG metadata.

    Reads the new embedded format written by spark_dsg's ``set_labelspace``::

        metadata["labelspaces"]["_l2p0"] = [[0, "unknown"], [1, "chair"], ...]
        metadata["labelspaces"]["_l4p0"] = [[0, "lounge"], [1, "hallway"], ...]

    Returns
    -------
    (object_ls, room_ls) : tuple[dict | None, dict | None]
        Each is ``{str(int_id): name}`` or ``None`` if not found.
    """
    meta = G.metadata.get()
    labelspaces = meta.get("labelspaces")
    if not labelspaces:
        return None, None

    object_ls = None
    room_ls = None

    obj_data = labelspaces.get("_l2p0")  # Objects: layer 2, partition 0
    if obj_data:
        object_ls = {str(pair[0]): pair[1] for pair in obj_data}

    room_data = labelspaces.get("_l4p0")  # Rooms: layer 4, partition 0
    if room_data:
        room_ls = {str(pair[0]): pair[1] for pair in room_data}

    return object_ls, room_ls


def load_dsg_to_db(neo4j_uri, neo4j_creds, scene_graph):
    """Load a DSG into Neo4j.

    Labelspaces must be embedded in the DSG metadata via ``set_labelspace()``.
    Use ``extract_labelspaces_from_dsg()`` to verify before calling.
    """
    # Set the layer id to layer name mappings
    spark_layer_id_to_heracles_layer_str = {
        2: "Object",
        5: "Building",
        20: "MeshPlace",
        "3[1]": "MeshPlace",
        3: "Place",
        4: "Room",
    }
    scene_graph.metadata.add(
        {"LayerIdToHeraclesLayerStr": spark_layer_id_to_heracles_layer_str}
    )

    with Neo4jWrapper(
        neo4j_uri, neo4j_creds, atomic_queries=True, print_profiles=False
    ) as db:
        # Clear any existing content from the DB & initialize with the schema
        print("Initializing the database.")
        initialize_db(db)
        # Load the scene graph into the DB
        print("Loading the scene graph into the database.")
        spark_dsg_to_db(scene_graph, db)
