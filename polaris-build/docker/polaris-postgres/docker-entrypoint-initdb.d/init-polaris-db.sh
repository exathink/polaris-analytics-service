#!/bin/bash
set -e

echo 'Running POLARIS DB Initializations..'

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE SCHEMA repos;
    CREATE SCHEMA polaris;
    CREATE SCHEMA auth;
    CREATE SCHEMA analytics;

    CREATE DATABASE polaris_test TEMPLATE=template0;
    \connect polaris_test;
    CREATE SCHEMA repos;
    CREATE SCHEMA polaris;
    CREATE SCHEMA auth;
    CREATE SCHEMA analytics;
    CREATE SCHEMA test;
    \q
EOSQL
