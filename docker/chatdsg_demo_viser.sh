#!/usr/bin/env bash
set -e

docker compose up -d neo4j viser_visualization
docker compose run --rm chatdsg
docker compose down viser_visualization
