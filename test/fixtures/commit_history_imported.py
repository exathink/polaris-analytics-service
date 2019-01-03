# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from test.constants import *

from datetime import datetime
import uuid
import pytest

from polaris.common import db
from polaris.analytics.db import model

joe_contributor_key = uuid.uuid4().hex
billy_contributor_key = uuid.uuid4().hex

commit_common_fields = dict(
    commit_date=datetime.utcnow(),
    commit_date_tz_offset=0,
    committer_alias_key=joe_contributor_key,
    author_date=datetime.utcnow(),
    author_date_tz_offset=0,
    author_alias_key=billy_contributor_key,
    created_at=datetime.utcnow(),
    commit_message='a change'

)

commit_history_imported_common = dict(
    organization_key=rails_organization_key,
    repository_name='rails',
    repository_key=rails_repository_key,
    branch_info=dict(
        name='master',
        is_new=False,
        is_default=True,
        is_stale=False,
        remote_head="ZZZZ",
        is_orphan=False
    )
)


@pytest.yield_fixture
def cleanup(setup_schema):
    yield

    db.connection().execute("delete from analytics.commits")
    db.connection().execute("delete from analytics.contributor_aliases")
    db.connection().execute("delete from analytics.contributors")
    db.connection().execute("delete from analytics.repositories")
    db.connection().execute("delete from analytics.organizations")


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

@pytest.yield_fixture()
def import_commit_details_fixture(setup_repo_org):
    repository_id, organization_id = setup_repo_org

    with db.create_session() as session:

        contributor_id = session.connection.execute(
            model.contributors.insert(
                dict(
                    name='Joe Blow',
                    key=joe_contributor_key
                )
            )
        ).inserted_primary_key[0]

        contributor_alias_id = session.connection.execute(
            model.contributor_aliases.insert(
                dict(
                    name='Joe Blow',
                    key=joe_contributor_key,
                    source='vcs',
                    source_alias='joe@blow.com',
                    contributor_id=contributor_id
                )
            )
        ).inserted_primary_key[0]
        keys = [uuid.uuid4() for i in range(1000, 1010)]
        session.connection.execute(
            model.commits.insert([
                dict(
                    repository_id=repository_id,
                    source_commit_id=f'{key}',
                    key=keys[1000-key],
                    committer_contributor_alias_id=contributor_alias_id,
                    author_contributor_alias_id=contributor_alias_id,
                    commit_date=datetime.utcnow(),
                    commit_date_tz_offset=0,
                    committer_contributor_key=joe_contributor_key,
                    committer_contributor_name="joe@blow.com",
                    author_date=datetime.utcnow(),
                    author_date_tz_offset=0,
                    author_contributor_key=billy_contributor_key,
                    author_contributor_name="billy",
                    created_at=datetime.utcnow(),
                    commit_message='a change'
                )
                for key in range(1000, 1010)
            ])
        )

    yield keys