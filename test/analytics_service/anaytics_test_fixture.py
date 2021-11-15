# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar



from polaris.repos.db.model import Account, Organization, Repository, Project, Contributor
from polaris.repos.db.schema import contributor_aliases

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


@pytest.fixture
def cleanup(setup_schema):
    yield

    db.connection().execute("delete from analytics.commits")
    db.connection().execute("delete from analytics.contributor_aliases")
    db.connection().execute("delete from analytics.contributors")
    db.connection().execute("delete from analytics.repositories")
    db.connection().execute("delete from analytics.organizations")


@pytest.fixture
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

