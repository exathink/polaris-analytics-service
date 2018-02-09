FROM python:3.5
MAINTAINER Krishna Kumar <kkumar@exathink.com>

RUN apt-get update \
    && apt-get install -y libxml2-dev libxslt1-dev libxmlsec1-dev libpq-dev \
    && rm -rf /var/lib/apt/lists/*

ENV POLARIS_ROOT "/build/tmp/polaris"

COPY docker/polaris-common-lib/requirements.txt \
    system/pip-constraints.txt \
    ${POLARIS_ROOT}/

COPY lib/polaris-setup ${POLARIS_ROOT}/polaris-setup/


WORKDIR ${POLARIS_ROOT}


RUN  pip install ./polaris-setup \
     && pip install  -r pip-constraints.txt  \
            && rm -rf /build/tmp



