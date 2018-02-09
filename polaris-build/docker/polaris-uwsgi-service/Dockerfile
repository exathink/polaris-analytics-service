ARG PACKAGE_IMAGE
FROM ${PACKAGE_IMAGE}

RUN groupadd -r uwsgi && useradd -r -g uwsgi uwsgi
USER uwsgi

COPY polaris-build/docker/polaris-uwsgi-service/start_uwsgi_service.sh /usr/local/bin/

ENTRYPOINT [ "start_uwsgi_service.sh" ]
