from setuptools import setup, find_packages

setup(
    name="heracles",
    version="0.0.1",
    url="https://github.com/npolshakova/heracles",
    author="Aaron Ray, Nina Polshakova",
    author_email="aaronray@mit.edu, ninapolshakova@gmail.com",
    description="Cypher / Neo4j support for SparkDSG Scene graphs",
    package_dir={"": "src"},
    packages=find_packages("src"),
    install_requires=["pyyaml", "neo4j-rust-ext"],
)
