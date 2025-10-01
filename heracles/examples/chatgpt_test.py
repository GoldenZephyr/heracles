import os
from importlib.resources import as_file, files

import openai
import yaml

import heracles.resources
from heracles.prompt_schema import Prompt

key = os.getenv("DSG_OPENAI_API_KEY")

client = openai.OpenAI(
    api_key=key,
    timeout=10,
)

with as_file(files(heracles.resources).joinpath("dsgdb_prompts.yaml")) as path:
    with open(str(path), "r") as fo:
        prompt_yaml = yaml.safe_load(fo)

prompt_obj = Prompt.from_dict(prompt_yaml)
print("Base prompt: ", prompt_obj)

prompt = prompt_obj.to_openai_json("Find all objects that are near a box and a bag")


print(prompt)

response = client.chat.completions.create(
    model="gpt-4o-mini",
    # model="gpt-4o",
    messages=prompt,
    temperature=0.1,
    seed=100,
    # response_format={"type": "text"},
    response_format={"type": "json_object"},
)

print("Response:\n\n")
print(response.choices[0].message.content)
