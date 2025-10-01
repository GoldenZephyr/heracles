import spark_dsg


def summarize_dsg(G):
    n_objects = len([o for o in G.get_layer(spark_dsg.DsgLayers.OBJECTS).nodes])
    n_places = len([o for o in G.get_layer(spark_dsg.DsgLayers.PLACES).nodes])
    try:
        n_2d_places = len(
            [o for o in G.get_layer(spark_dsg.DsgLayers.MESH_PLACES).nodes]
        )
    except IndexError:
        # Backwards compatibility
        n_2d_places = len([o for o in G.get_layer(20).nodes])
    n_rooms = len([o for o in G.get_layer(spark_dsg.DsgLayers.ROOMS).nodes])
    print("===== DSG Summary =====")
    print("# Objects: ", n_objects)
    print("# Places: ", n_places)
    print("# 2D Places: ", n_2d_places)
    print("# Rooms: ", n_rooms)
