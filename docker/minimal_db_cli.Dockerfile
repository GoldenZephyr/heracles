FROM python:3.12-slim

RUN apt update
COPY heracles heracles
RUN pip install ./heracles ipython
