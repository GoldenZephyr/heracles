#!/usr/bin/env bash
set -e

docker compose -f docker/docker-compose.yaml up -d neo4j hydra_visualization
docker compose -f docker/docker-compose.yaml run --rm chatdsg # Blocks until terminal exits
docker compose -f docker/docker-compose.yaml down hydra_visualization
