#!/usr/bin/env bash
set -e
docker compose -f docker/docker-compose.yaml up -d neo4j
docker compose -f docker/docker-compose.yaml run --rm cli
