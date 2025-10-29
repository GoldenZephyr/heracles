#!/usr/bin/env bash
set -e
docker compose up -d neo4j
docker compose run --rm cli
