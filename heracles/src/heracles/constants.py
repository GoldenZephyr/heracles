from types import MappingProxyType

import spark_dsg

# Constant strings for referencing layers in heracles
MESH_PLACES = "MeshPlace"
PLACES = "Place"
OBJECTS = "Object"
ROOMS = "Room"
BUILDINGS = "Building"

# Mappings to/from heracles and spark_dsg
SPARK_TO_HERACLES_LAYER_NAMES = MappingProxyType(
    {
        spark_dsg.DsgLayers.MESH_PLACES: MESH_PLACES,
        spark_dsg.DsgLayers.PLACES: PLACES,
        spark_dsg.DsgLayers.OBJECTS: OBJECTS,
        spark_dsg.DsgLayers.ROOMS: ROOMS,
        spark_dsg.DsgLayers.BUILDINGS: BUILDINGS,
    }
)
HERACLES_TO_SPARK_LAYER_NAMES = MappingProxyType(
    {
        MESH_PLACES: spark_dsg.DsgLayers.MESH_PLACES,
        PLACES: spark_dsg.DsgLayers.PLACES,
        OBJECTS: spark_dsg.DsgLayers.OBJECTS,
        ROOMS: spark_dsg.DsgLayers.ROOMS,
        BUILDINGS: spark_dsg.DsgLayers.BUILDINGS,
    }
)
