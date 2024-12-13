# Heracles



## Setup

Pull the image:
```bash
docker pull neo4j:5.25.1
```

Run the database:
```bash
 docker run -d \
    --restart always \
    --publish=7474:7474 --publish=7687:7687 \
    neo4j:5.25.1
```
