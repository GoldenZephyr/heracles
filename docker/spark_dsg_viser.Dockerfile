FROM python:3.12-slim

RUN apt update
RUN apt install -y git libzmq3-dev nlohmann-json3-dev build-essential
COPY heracles heracles
RUN git clone -b feature/viser_aaron --single-branch https://github.com/MIT-SPARK/Spark-DSG.git spark_dsg
RUN pip install ./heracles ipython ./spark_dsg
