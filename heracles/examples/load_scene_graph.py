#!/usr/bin/env python3
import argparse
import os

import spark_dsg

from heracles.dsg_utils import summarize_dsg
from heracles.query_interface import Neo4jWrapper
from heracles.utils import load_dsg_to_db

if __name__ == "__main__":
    # Check for DB authentication credentials
    neo4j_user = os.getenv("HERACLES_NEO4J_USERNAME")
    assert neo4j_user, '"$HERACLES_NEO4J_USERNAME" is not set.'
    neo4j_pw = os.getenv("HERACLES_NEO4J_PASSWORD")
    assert neo4j_pw, '"$HERACLES_NEO4J_PASSWORD" is not set.'
    neo4j_creds = (neo4j_user, neo4j_pw)
    # Parse the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--scene_graph", type=str, default="scene_graphs/example_dsg.json"
    )
    parser.add_argument(
        "--neo4j_uri", type=str, default=os.getenv("HERACLES_NEO4J_URI")
    )
    parser.add_argument(
        "--object_labelspace", type=str, default="ade20k_mit_label_space.yaml"
    )
    parser.add_argument("--room_labelspace", type=str, default="b45_label_space.yaml")
    args = parser.parse_args()
    assert args.neo4j_uri, (
        'No NEO4J_URI provided -- either provide as an arg or set "$HERACLES_NEO4J_URI"'
    )
    # Load the scene graph from file
    print(f'Loading the scene graph from file ("{args.scene_graph}").')
    scene_graph = spark_dsg.DynamicSceneGraph.load(args.scene_graph)
    print("Done loading the scene graph from file.")
    summarize_dsg(scene_graph)

    load_dsg_to_db(
        args.object_labelspace,
        args.room_labelspace,
        args.neo4j_uri,
        neo4j_creds,
        scene_graph,
    )

    db = Neo4jWrapper(
        args.neo4j_uri, neo4j_creds, atomic_queries=True, print_profiles=False
    )
    db.connect()

    # Print the distinct object classes and their count
    print("Querying for all of the distinct object classes and their count.")
    records, summary, keys = db.execute(
        """MATCH (n: Object) RETURN DISTINCT n.class as class, COUNT(*) as count"""
    )
    for record in records:
        print(record.data())
