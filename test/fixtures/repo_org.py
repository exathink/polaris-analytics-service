# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import pytest

from polaris.analytics.db import model
from polaris.common import db
from test.constants import rails_organization_key, rails_repository_key


@pytest.yield_fixture
def cleanup(setup_schema):
    yield
    db.connection().execute("delete from analytics.work_item_source_file_changes")
    db.connection().execute("delete from analytics.work_item_delivery_cycle_contributors")
    db.connection().execute("delete from analytics.work_item_delivery_cycle_durations")
    db.connection().execute("delete from analytics.work_item_delivery_cycles")
    db.connection().execute("delete from analytics.work_items_source_state_map")
    db.connection().execute("delete from analytics.work_item_state_transitions")
    db.connection().execute("delete from analytics.work_items")
    db.connection().execute("delete from analytics.work_items_sources")
    db.connection().execute("delete from analytics.commits")
    db.connection().execute("delete from analytics.source_files")
    db.connection().execute("delete from analytics.contributor_aliases")
    db.connection().execute("delete from analytics.contributors")
    db.connection().execute("delete from analytics.projects_repositories")
    db.connection().execute("delete from analytics.repositories")
    db.connection().execute("delete from analytics.projects")
    db.connection().execute("delete from analytics.organizations")

@pytest.yield_fixture()
def setup_org(cleanup):
    with db.orm_session() as session:
        session.expire_on_commit = False
        organization = model.Organization(
            name='rails',
            key=rails_organization_key
        )
        session.add(organization)

    yield organization


@pytest.yield_fixture
def setup_repo_org(cleanup):
    with db.create_session() as session:
        organization_id = session.connection.execute(
            model.organizations.insert(
                dict(
                    name='rails',
                    key=rails_organization_key
                )
            )
        ).inserted_primary_key[0]

        repository_id = session.connection.execute(
            model.repositories.insert(
                dict(
                    name='rails',
                    key=rails_repository_key,
                    url='foo',
                    organization_id=organization_id
                )
            )
        ).inserted_primary_key[0]

    yield repository_id, organization_id