FROM polaris-bootstrap:latest
MAINTAINER Krishna Kumar <kkumar@exathink.com>

# Build context is polaris-common



COPY docker/polaris-base/requirements.txt \
     system/pip-constraints.txt \
     lib/polaris-setup \
     /build/tmp/polaris-setup/

WORKDIR /build/tmp/polaris-setup

RUN pip install --upgrade pip && \
    pip install -r requirements.txt -c pip-constraints.txt && \
    pip install . && rm -rf /build/tmp

