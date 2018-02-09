FROM python:2.7-slim
MAINTAINER Krishna Kumar <kkumar@exathink.com>

RUN apt-get update \
    && apt-get install -y libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*