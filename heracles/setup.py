from setuptools import find_packages, setup

setup(
    name="heracles",
    version="0.0.1",
    url="https://github.com/npolshakova/heracles",
    author="Aaron Ray, Nina Polshakova",
    author_email="aaronray@mit.edu, ninapolshakova@gmail.com",
    description="Cypher / Neo4j support for SparkDSG Scene graphs",
    package_dir={"": "src"},
    packages=find_packages("src"),
    package_data={"": ["*.yaml"]},
    install_requires=[
        "pyyaml",
        "neo4j-rust-ext",
        "numpy",
        "pytest",
        "openai",
        "parse",
        "open3d",
        "spark-dsg @ git+https://github.com/MIT-SPARK/Spark-DSG.git@feature/viser",
    ],
)
