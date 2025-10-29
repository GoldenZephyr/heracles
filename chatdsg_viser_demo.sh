#!/usr/bin/env bash
set -e

docker compose -f docker/docker-compose.yaml up -d neo4j viser_visualization
docker compose -f docker/docker-compose.yaml run --rm chatdsg
docker compose -f docker/docker-compose.yaml down viser_visualization
