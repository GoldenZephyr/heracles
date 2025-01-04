import openai
import os


key = os.getenv("DSG_OPENAI_API_KEY")

client = openai.OpenAI(
    api_key=key,
    timeout=10,
)

prompt = []

developer_prompt = """
You are a helpful assistant who is an expert at mapping from natural language queries to Cypher queries for a Neo4j graph database. You have access to a database representing a 3D scene graph, which stores spatial information that robot can use to understand the world. Given a query, your task is to generate a Cypher query that queries the relevant information from the database. 

Labels in Database:
    - Object: a node representing an object in the world.
      Object Properties:
        - nodeSymbol: a unique string identifier
        - class: a string identifying the object's semantic class
        - center: the 3D position of the object
    - MeshPlace: a node representing a 2D segment of space the robot might be able to move to.
        - nodeSymbol: a unique string identifier
        - class: a string identifying the place's semantic class
        - center: the 3D position of the mesh place
    - Place: a node representing a 3D region of free space
        - nodeSymbol: a unique string identifier
        - center: the 3D position of the place
    - Room: a node representing a room or higher-level region
        - nodeSymbol: a unique string identifier
        - center: the 3D position of the room

Places and Mesh Places represent a higher level of the hierarchy compared to objects, but lower level than rooms. 

There are two kinds of existing edges. First is (a)-[:CONTAINS]->(b), which connects nodes between different layers and means that b is contained within a. Nodes in higher levels of the hierarchy may contain nodes in lower levels of the hierarchy, but nodes in the lower level of the hierarchy will not contain higher-level nodes. The other kind of edges represent connectivity within a layer: [:OBJECT_CONNECTED], [:PLACE_CONNECTED], [:MESH_PLACE_CONNECTED], [:ROOM_CONNECTED].
"""

prompt.append({"role": "developer", "content": developer_prompt})

icl0_user = """
The following is a set of examples of mapping instructions to Cypher queries:

Print distinct object classes in scene graph
"""
prompt.append({"role": "user", "content": icl0_user})

icl0_assistant = """
MATCH (n: Object) RETURN DISTINCT n.class as class
"""
prompt.append({"role": "assistant", "content": icl0_assistant})

icl1_user = "Print distinct object classes and counts"
icl1_assistant = """
MATCH (n: Object) RETURN DISTINCT n.class as class, COUNT(*) as count
"""

prompt.append({"role": "user", "content": icl1_user})
prompt.append({"role": "assistant", "content": icl1_assistant})

icl2_user = """
Find locations of all trees
"""
icl2_assistant = """
MATCH (n: Object {class: 'tree'}) RETURN n.center as center
"""

prompt.append({"role": "user", "content": icl2_user})
prompt.append({"role": "assistant", "content": icl2_assistant})

icl3_user = """
Find all trees withing bouding box with corners [-100, 16, -100] and [-90, 22, 100]
"""

icl3_assistant = """
WITH point({x: -100, y: 16, z: -100}) AS lowerLeft,
     point({x: -90, y: 22, z:100}) AS upperRight
MATCH (t: Object {class: "tree"})
WHERE point.withinBBox(t.center, lowerLeft, upperRight)
RETURN t as result
"""

prompt.append({"role": "user", "content": icl3_user})
prompt.append({"role": "assistant", "content": icl3_assistant})

icl4_user = """
Find the trees within 30 meters of [-100, 16, 0]
"""

icl4_assistant = """
MATCH (t: Object {class: "tree"})
WITH point.distance(point({x: -100, y:16, z:0}), t.center) as d, t
WHERE d < 30
 RETURN t as obj, d as distance
 """

prompt.append({"role": "user", "content": icl4_user})
prompt.append({"role": "assistant", "content": icl4_assistant})

icl5_user = """
Get all mesh places within 5 hops of place P(1832)
"""
icl5_assistant = """
MATCH (p: MeshPlace {nodeSymbol: "P(1832)"})
MATCH path=(p)-[:MESH_PLACE_CONNECTED *1..5]->(c: MeshPlace)
RETURN DISTINCT c.nodeSymbol as ns
ORDER BY c.nodeSymbol
"""

prompt.append({"role": "user", "content": icl5_user})
prompt.append({"role": "assistant", "content": icl5_assistant})

icl6_user = """
Create a Room at [-95, 15, 0] with class test_room and node symbol R(1)
"""

icl6_assistant = """
WITH point({x: -95, y: 15, z: 0}) as center
CREATE (:Room {nodeSymbol: "R(1)", center: center,  class: "test_room"})
"""

prompt.append({"role": "user", "content": icl6_user})
prompt.append({"role": "assistant", "content": icl6_assistant})

icl7_user = """
Connect place near room to room
Add edges so that all places within the bounding box [-120, 0, -100], [-70, 30, 100] are contained in Room R(1)
"""
icl7_assistant = """
WITH point({x: -120, y: 0, z: -100}) AS lowerLeft,
     point({x: -70, y: 30, z:100}) AS upperRight
MATCH (p: Place)
MATCH (r: Room {nodeSymbol: "R(1)"})
WHERE point.withinBBox(p.center, lowerLeft, upperRight)
CREATE (r)-[:CONTAINS]->(p)
"""

prompt.append({"role": "user", "content": icl7_user})
prompt.append({"role": "assistant", "content": icl7_assistant})

icl8_user = """
Get all objects contained within Room R(1)
"""

icl8_assistant = """
MATCH (r: Room {nodeSymbol: "R(1)"})-[:CONTAINS*]->(o: Object)
RETURN o
"""

prompt.append({"role": "user", "content": icl8_user})
prompt.append({"role": "assistant", "content": icl8_assistant})

novel_instruction = """
Now, generate a cypher query for this natural language query:\n
"""

# novel_instruction += "Find the location of all boxes"
novel_instruction += "Find all objects that are near a box and a bag"
prompt.append({"role": "user", "content": novel_instruction})


print(prompt)

response = client.chat.completions.create(
    #model="gpt-4o-mini",
    model="gpt-4o",
    messages=prompt,
    temperature=0.0,
    seed=100,
    response_format={"type": "text"},
)

print("\n\n")
print(response.choices[0].message.content)
