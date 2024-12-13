from neo4j import GraphDatabase

URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "neo4jiscool")

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()

    driver.execute_query(
        "MATCH (n) DETACH DELETE n",
        database_="neo4j",
    )

    # TODO(npolshak): Figure out how to load data in a more sane way
    parameters = [ 
        {"nodeSymbol": "p(29)", "x": 15, "y": 10, "z": 0, "layer": "surface_place", "class": "road"},
        {"nodeSymbol": "p(30)", "x": 20, "y": 10, "z": 0, "layer": "surface_place", "class": "road"},
        {"nodeSymbol": "p(31)", "x": 25, "y": 10, "z": 0, "layer": "surface_place", "class": "road"},
        {"nodeSymbol": "p(32)", "x": 30, "y": 10, "z": 0, "layer": "surface_place", "class": "road"},
        {"nodeSymbol": "p(33)", "x": 35, "y": 10, "z": 0, "layer": "surface_place", "class": "road"},
        {"nodeSymbol": "p(34)", "x": 25, "y": 15, "z": 0, "layer": "surface_place", "class": "gravel"},
        {"nodeSymbol": "p(35)", "x": 25, "y": 5, "z": 0, "layer": "surface_place", "class": "gravel"},
        {"nodeSymbol": "o(1)", "x": 30.5, "y": 10, "z": 0, "layer": "object", "class": "bag"},
        {"nodeSymbol": "o(2)", "x": 25.5, "y": 15, "z": 0, "layer": "object", "class": "book"},
        
    ]
    for param in parameters:
        results = driver.execute_query(
            """
            WITH point({x: $x, y: $y, z: $z}) AS p3d
            MERGE (:Scene {nodeSymbol: $nodeSymbol, center: p3d, layer: $layer, class: $class})
            """,
            parameters_=param,
            database_="neo4j",
        )

        print(
            "Created {nodes_created} nodes in {time} ms.".format(
                nodes_created=results.summary.counters.nodes_created,
                time=results.summary.result_available_after,
            )
        )

    records, summary, keys = driver.execute_query(
        "MATCH (p:Scene) RETURN p.nodeSymbol AS nodeSymbol",
        database_="neo4j",
    )

    # Loop through results and do something with them
    for record in records:
        print(record.data())  # obtain record as dict

    # list of edges
    surface_parameters = [
        ("p(29)", "p(30)"),
        ("p(30)", "p(31)"),
        ("p(31)", "p(32)"),
        ("p(32)", "p(33)"),
        ("p(31)", "p(34)"),
        ("p(34)", "p(35)"),
    ]
    for place1, place2 in surface_parameters:
        records1, summary1, keys1 = driver.execute_query(
            """
        MATCH (place_one:Scene {nodeSymbol: $place_one})
        MATCH (place_two:Scene {nodeSymbol: $place_two})
        CREATE (place_one)-[:CONNECTED]->(place_two)
        """,
            place_one=place1,
            place_two=place2,
            database_="neo4j",
        )
        print(f"Query counters: {summary1.counters}.")

        records2, summary2, keys2 = driver.execute_query(
            """
        MATCH (place_one:Scene {nodeSymbol: $place_one})
        MATCH (place_two:Scene {nodeSymbol: $place_two})
        CREATE (place_one)<-[:CONNECTED]-(place_two)
        """,
            place_one=place1,
            place_two=place2,
            database_="neo4j",
        )
        print(f"Query counters: {summary2.counters}.")


    # list of edges
    obj_parameters = [
        ("o(1)", "p(32)"),
        ("o(2)", "p(34)"),
    ]
    for obj, place in obj_parameters:
        records, summary, keys = driver.execute_query(
            """
        MATCH (obj:Scene {nodeSymbol: $obj})
        MATCH (place:Scene {nodeSymbol: $place})
        CREATE (obj)-[:IN]->(place)
        """,
            obj=obj,
            place=place,
            database_="neo4j",
        )
        print(f"Query counters: {summary.counters}.")

    records, summary, keys = driver.execute_query(
    """MATCH (n1:Scene)-[:CONNECTED]-(n2:Scene)
    WHERE n1.center.x - n2.center.x > 1
    RETURN n1.nodeSymbol AS node1, n2.nodeSymbol as node2""",
    database_="neo4j",
    )
    print("connected nodes:")
    for record in records:
        print(record.data())  # obtain record as dict

    # obj in a gravel place?
    records, summary, keys = driver.execute_query(
    """MATCH (obj:Scene {layer: "object"} )-[:IN]-(place:Scene {class: "gravel"})
    RETURN obj.nodeSymbol AS obj, place.nodeSymbol as place""",
    database_="neo4j",
    )
    print("grave object nodes:")
    for record in records:
        print(record.data())  # obtain record as dict

    #WHERE (place)-[:CONNECTED*1..2]-(init_place)
    # paths to objects within 2 hops
    records, summary, keys = driver.execute_query(
    """MATCH (obj:Scene {layer: "object"} )-[:IN]-(place:Scene)
       MATCH (init_place:Scene {nodeSymbol: $nodeSymbol})
       MATCH p = SHORTEST 1 (place)<-[:CONNECTED*..10]-(init_place)
    RETURN obj.nodeSymbol AS obj, p AS path""",
    nodeSymbol="p(30)",
    database_="neo4j",
    )
    print("feasible objects:")
    for record in records:
        print(record.data())  # obtain record as dict
    

    # distance between objects 
    # RETURN obj1.nodeSymbol AS obj1, obj2.nodeSymbol AS obj2, d AS distance
    records, summary, keys = driver.execute_query(
    """
    MATCH (obj1:Scene {layer: "object"} ), (obj2:Scene {layer: "object"} )
    WHERE obj1 <> obj2
    RETURN obj1.nodeSymbol AS first_obj, obj2.nodeSymbol AS sec_obj, point.distance(obj1.center, obj2.center) as d""",
    database_="neo4j",
    )
    print("feasible objects:")
    for record in records:
        print(record.data())  # obtain record as dict