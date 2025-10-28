<p align="center">
  <img src="img/heracles.png" alt="Heracles Logo" width="120"/><br>
  <b>Heracles</b>
</p>

# What is Heracles?
This repository provides a library for encoding a [Hydra](https://github.com/MIT-SPARK/Hydra) 3D scene graph into a Neo4j database. It expects a scene graph to initially be loaded from file via [spark\_dsg](https://github.com/MIT-SPARK/Spark-DSG) and provides utilities for translating back and forth between spark\_dsg and the Neo4j database.

## Setup

Pull the Neo4j database image:
```bash
docker pull neo4j:5.25.1
```

Run the database:
```bash
docker run -d \
   --restart always \
   --publish=7474:7474 --publish=7687:7687 \
   --env=NEO4J_AUTH=neo4j/neo4j_pw \
   neo4j:5.25.1
```

Clone and install this repo:
```bash
git@github.com:GoldenZephyr/heracles.git
cd heracles
pip install .
```

If you run into installation problems, check `.github/workflows/ci-action.yml`
for an example, as that successfully installs and runs the library for CI.

If you don't have a scene graph to test with, you can use a large example scene
graph [here](https://drive.google.com/file/d/1aktyS792PUrj2ACRu1DoxMGse55GWloB/view?usp=drive_link).
This scene graph has 2D places and objects, and 3D places (that don't make much
sense), but no regions or rooms.

The script `examples/dsg_test.py` is a small example of loading an existing
scene graph file into a graph database and running some simple queries. You can
run it in interaction mode (`ipython3 -i heracles/examples/dsg_test.py`) and
then try executing some of the other example queries below.

## Demo - Loading a Scene Graph
We provide a demo program that loads a scene graph from file, encodes it into a database, and performs a simple query for validation. The demo program is implemented with interactive mode in mind to make it easy to try other kinds of queries.

### Environment Variables
We expect certain environment variables to be set, listed below. `HERACLES\_NEO4J\_URI` can alternatively be provided via the command line. An example of `HERACLES\_NEO4J\_URI` is `neo4j://127.0.0.1:7688`.

| Environment Variable Name         | Description                                                                |
|-----------------------------------|----------------------------------------------------------------------------|
| HERACLES\_NEO4J\_USERNAME         | Username of local Neo4j graph database                                     |
| HERACLES\_NEO4J\_PASSWORD         | Password of local Neo4j graph database                                     |
| HERACLES\_NEO4J\_URI              | Address for database (neo4j://IP:PORT)                                     |


### Run the program
To run the vanilla demo (load a scene graph and print a summary):
```bash
cd heracles/examples/
./load_scene_graph.py
```

The demo is useful to run in interactive mode. We leave the DB connection open, so it is possible to easily try other queries.
To run the program in interactive mode:
```bash
cd heracles/examples/
python -i load_scene_graph.py
```
The demo should run as normal but end in an active interactive mode. You can immediately try another query. For example:
```bash
db.query("MATCH (n: Object) RETURN DISTINCT n.class as class, COUNT(*) as count")

## Heracles ROS

If you would like to use `heracles` with ROS, `heracles_ros` provides an
interface for publishing the scene graph from the graph database as a ROS
message. If you would like to use this functionality, you should clone this
repo in your ROS workspace. Instead of manually pip installing as described
above, you can let `colcon` take care of the installation (as long as your
virtual environment is created with `--system-site-packages`). If you also have
[Hydra](https://github.com/MIT-SPARK/Hydra-ROS/tree/main) installed, you can
visualize the scene graph that's in the graph database with the following
command:

```bash
ros2 launch heracles_ros demo.launch.yaml launch_heracles_publisher:=true launch_hydra_visualizer:=true launch_rviz:=true
```

## Useful Queries


### Print distinct object classes in scene graph

```python
db.query("MATCH (n: Object) RETURN DISTINCT n.class as class""")
```

### Print distinct object classes and counts
```python
db.query("MATCH (n: Object) RETURN DISTINCT n.class as class, COUNT(*) as count""")
```

### Print center point of objects of specific class
```python
db.query("MATCH (n: Object {class: 'tree'}) RETURN n.center as center""")
```

### Filter for objects of a certain type within a bounding box
```python
db.query("""WITH point({x: -100, y: 16, z: -100}) AS lowerLeft,
                 point({x: -90, y: 22, z:100}) AS upperRight
            MATCH (t: Object {class: "tree"})
            WHERE point.withinBBox(t.center, lowerLeft, upperRight)
            RETURN t as result""")
```

### Filter for all objects of a certain type within 30 meters of given point
```python
db.query(""" MATCH (t: Object {class: "tree"})
             WITH point.distance(point({x: -100, y:16, z:0}), t.center) as d, t
             WHERE d < 30
             RETURN t as obj, d as distance""")
```

### Get all 2D places within 5 hops of given place

For graph-based reasoning with larger neighborhoods, there are some special
built-in graph search algorithms that will be more performant that these
general cypher queries. For example, for connected-component queries, you
probably want to use [the graph-data-science
plugin](https://neo4j.com/docs/graph-data-science/current/algorithms/wcc/)
```python
db.query(""" MATCH (p: MeshPlace {nodeSymbol: "P(1832)"})
             MATCH path=(p)-[:MESH_PLACE_CONNECTED *1..5]->(c: MeshPlace)
             RETURN DISTINCT c.nodeSymbol as ns
             ORDER BY c.nodeSymbol
         """)
```

### Create a Room

Potentially useful for directly modifying data with LLM-generated prompt. For a
more robust but less general way of adding nodes see `graph_interface.py`.

```python
db.query("""WITH point({x: -95, y: 15, z: 0}) as center
            CREATE (:Room {nodeSymbol: "R(1)", center: center,  class: "test_room"})""")
```

### Connect place near room to room

```python
db.query("""WITH point({x: -120, y: 0, z: -100}) AS lowerLeft,
                 point({x: -70, y: 30, z:100}) AS upperRight
            MATCH (p: Place)
            MATCH (r: Room {nodeSymbol: "R(1)"})
            WHERE point.withinBBox(p.center, lowerLeft, upperRight)
            CREATE (r)-[:CONTAINS]->(p)
            """)
```

### Query for objects in Room

Based on *explicit* structure of Room -> Place -> Object
```python
db.query("""MATCH (r: Room {nodeSymbol: "R(1)"})-[:CONTAINS]->(p: Place)-[:CONTAINS]->(o: Object)
            RETURN o""")
```

Based on implicit / transitive structure of hierarchical relationships:
```python
db.query("""MATCH (r: Room {nodeSymbol: "R(1)"})-[:CONTAINS*]->(o: Object)
            RETURN o""")
```
