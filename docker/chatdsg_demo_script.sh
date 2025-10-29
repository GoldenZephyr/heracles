#!/usr/bin/env bash
set -e

docker compose up -d neo4j hydra_visualization
docker compose run --rm chatdsg # Blocks until terminal exits
docker compose down hydra_visualization
