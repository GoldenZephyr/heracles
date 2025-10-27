import logging
import sys
from importlib.resources import as_file, files

import spark_dsg
import yaml
from heracles_evaluation.llm_agent import LlmAgent
from heracles_evaluation.llm_interface import AgentContext
from prompt_toolkit import prompt

import heracles.constants
import heracles.resources
from heracles.graph_interface import initialize_db, spark_dsg_to_db
from heracles.query_interface import Neo4jWrapper


class InfoToPrintHandler(logging.Handler):
    def emit(self, record):
        if record.levelno == logging.INFO:
            print(record.getMessage())


def new_user_message(text):
    return [{"role": "user", "content": text}]


def add_metadata(H):
    # Load the object and room/region labelspaces
    with as_file(
        files(heracles.resources).joinpath("ade20k_mit_label_space.yaml")
    ) as path:
        with open(str(path), "r") as fo:
            object_labelspace = yaml.safe_load(fo)
    id_to_object_label = {
        item["label"]: item["name"] for item in object_labelspace["label_names"]
    }
    H.metadata.add({"labelspace": id_to_object_label})

    # with as_file(files(heracles.resources).joinpath("scene_camp_buckner_label_space.yaml")) as path:
    with as_file(files(heracles.resources).joinpath("b45_label_space.yaml")) as path:
        with open(str(path), "r") as fo:
            room_labelspace = yaml.safe_load(fo)
    id_to_room_label = {
        item["label"]: item["name"] for item in room_labelspace["label_names"]
    }
    H.metadata.add({"room_labelspace": id_to_room_label})

    # Define a layer id map
    layer_id_to_layer_name = {
        20: heracles.constants.MESH_PLACES,
        "3[1]": heracles.constants.MESH_PLACES,
        3: heracles.constants.PLACES,
        2: heracles.constants.OBJECTS,
        4: heracles.constants.ROOMS,
        5: heracles.constants.BUILDINGS,
    }
    print(layer_id_to_layer_name)

    H.metadata.add({"LayerIdToHeraclesLayerStr": layer_id_to_layer_name})


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
for h in logger.handlers[:]:
    logger.removeHandler(h)
logger.addHandler(InfoToPrintHandler())


def generate_initial_prompt(agent: LlmAgent):
    prompt = agent.agent_info.prompt_settings.base_prompt
    return prompt


if __name__ == "__main__":
    if len(sys.argv) > 2:
        print("Usage: ./chatgpt_agent.py <dsg filepath>")
        exit(1)

    with open("agent_config.yaml", "r") as fo:
        yml = yaml.safe_load(fo)
    agent_config = LlmAgent(**yml)
    if len(sys.argv) == 2:
        G = spark_dsg.DynamicSceneGraph.load(sys.argv[1])
        add_metadata(G)

        with Neo4jWrapper(
            "neo4j://127.0.0.1:7686",
            ("neo4j", "neo4j_pw"),
            atomic_queries=True,
            print_profiles=False,
        ) as db:
            initialize_db(db)
            spark_dsg_to_db(G, db)

    messages = generate_initial_prompt(agent_config).to_openai_json()
    logger.info(f"\nLLM Prompt: {messages}\n")

    print("You will be asked for input. Press <Esc> <enter> to submit input")
    print("You can exit by typing exit")
    while True:
        user_input = prompt("Give me some input: ", multiline=True)
        if user_input == "exit":
            break

        messages += new_user_message(user_input)

        cxt = AgentContext(agent_config)
        cxt.history = messages
        # cxt.initialize_agent(messages)
        success, answer = cxt.run()
        print(f"\nLLM says: {answer}\n")
        messages = cxt.history

    print("Bye!")
