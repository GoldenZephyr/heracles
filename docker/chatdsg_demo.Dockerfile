FROM python:3.12-slim

RUN apt-get update
RUN apt-get install -y git
COPY heracles/examples/scene_graphs/example_dsg.json example_dsg.json
RUN git clone --branch feature/chatdsg_optional_dsg_reset https://github.com/GoldenZephyr/heracles_agents.git
RUN pip install ./heracles_agents[all]
WORKDIR /heracles_agents/examples/chatdsg
