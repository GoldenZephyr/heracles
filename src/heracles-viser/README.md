# Setup

1. neo4j running in docker:

```
docker run -d \
  --restart always \
  --net=host \
  --env NEO4J_server_bolt_listen__address=0.0.0.0:7683 \
  --env NEO4J_server_bolt_advertised__address=127.0.0.1:7683 \
  --env NEO4J_server_http_listen__address=127.0.0.1:7469 \
  --env NEO4J_server_http_advertised__address=127.0.0.1:7470 \
  --env NEO4J_AUTH=neo4j/neo4j_pw \
  --name example_neo4j_db \
  neo4j:5.25.1
```

Load the scene graph into the database

2. Then connect with default values:
```
python src/heracles-viser/heracles_viser_publisher.py
```

The visualizer will run on localhost:8080
