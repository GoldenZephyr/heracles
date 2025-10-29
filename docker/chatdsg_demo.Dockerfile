FROM python:3.12-slim

RUN apt-get update
RUN apt-get install -y git
#COPY heracles heracles
RUN git clone https://github.com/GoldenZephyr/heracles_agents.git
RUN pip install ./heracles_agents[all]
WORKDIR /heracles_agents/examples/chatdsg
