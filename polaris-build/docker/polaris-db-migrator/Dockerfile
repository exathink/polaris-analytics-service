ARG PACKAGE_IMAGE
FROM ${PACKAGE_IMAGE}

ARG ALEMBIC_ROOT
ENV ALEMBIC_ROOT ${ALEMBIC_ROOT:-.}

COPY ${ALEMBIC_ROOT}/alembic.ini /package_migrations/
COPY ${ALEMBIC_ROOT}/migrations /package_migrations/migrations/
WORKDIR /package_migrations

ENTRYPOINT ["alembic"]
