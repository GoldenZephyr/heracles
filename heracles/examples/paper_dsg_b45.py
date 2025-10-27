import numpy as np
import spark_dsg

# import pydsg
import yaml

G = spark_dsg.DynamicSceneGraph.load(
    "/home/ubuntu/lxc_datashare/heracles_evaluation_dsg_data/postprocess1/b45_clip_2_mesh.json"
)

ll_clip = (-27, -27)
ur_clip = (8.5, -1.1)

nodes_to_remove = []
for n in G.nodes:
    p = n.attributes.position
    if p[0] < ll_clip[0]:
        nodes_to_remove.append(n.id.value)
        continue
    if p[0] > ur_clip[0]:
        nodes_to_remove.append(n.id.value)
        continue
    if p[1] < ll_clip[1]:
        nodes_to_remove.append(n.id.value)
        continue
    if p[1] > ur_clip[1]:
        nodes_to_remove.append(n.id.value)
        continue

for n in nodes_to_remove:
    G.remove_node(n)

classes_to_remove = [24, 29]
objects_to_remove = []
for n in G.get_layer(spark_dsg.DsgLayers.OBJECTS).nodes:
    if n.attributes.semantic_label in classes_to_remove:
        objects_to_remove.append(n.id.value)

for n in objects_to_remove:
    G.remove_node(n)

c = 0

for n in G.get_layer(spark_dsg.DsgLayers.OBJECTS).nodes:
    c += 1
print("total objects: ", c)

bb_fn = "/home/ubuntu/lxc_datashare/heracles_evaluation_dsg_data/gt_room_bounding_boxes_heracles.yaml"

with open(bb_fn, "r") as fo:
    box_yaml = yaml.safe_load(fo)


for room_id in box_yaml:
    attrs = spark_dsg.RoomNodeAttributes()
    attrs.name = f"R{room_id}"
    G.add_node(
        spark_dsg.DsgLayers.ROOMS, spark_dsg.NodeSymbol("R", int(room_id)), attrs
    )

for room_id, boxes in box_yaml.items():
    for b in boxes:
        extents = b["extents"]
        extents[2] = 20
        bb = spark_dsg.BoundingBox(extents, b["center"])
        print("bb: ", bb)
        for n in G.get_layer(spark_dsg.DsgLayers.MESH_PLACES).nodes:
            if bb.contains(n.attributes.position):
                print(
                    "adding edge: ", n.id.value, spark_dsg.NodeSymbol("R", int(room_id))
                )
                G.insert_edge(n.id.value, spark_dsg.NodeSymbol("R", int(room_id)).value)

node_to_position = {}
for n in G.get_layer(spark_dsg.DsgLayers.ROOMS).nodes:
    center = np.zeros(3)
    count = 0
    for c in n.children():
        child = G.get_node(c)
        center += child.attributes.position
        count += 1
    center /= count
    print("center: ", center)
    n.attributes.position = center


room_id_to_label_idx = {"1": 0, "2": 1, "3": 1, "4": 1, "5": 1}
for rid, label in room_id_to_label_idx.items():
    v = spark_dsg.NodeSymbol("R", int(rid)).value
    n = G.get_node(v)
    n.attributes.semantic_label = label


G.save("b45_out_dsg.json")
