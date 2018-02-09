FROM postgres:9.5
MAINTAINER Krishna Kumar <kkumar@exathink.com>

# Set the default user name and password. Default DB will also be same as user.
# The default password should be changed from the run command when not in a development environment

ENV POSTGRES_USER="polaris" POSTGRES_PASSWORD="polaris"
COPY docker-entrypoint-initdb.d /docker-entrypoint-initdb.d/
