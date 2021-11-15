# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from datetime import datetime
from test.constants import joe_contributor_key, billy_contributor_key
from test.fixtures.repo_org import *
import pytest

from polaris.analytics.db import model
from polaris.common import db


@pytest.fixture()
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
                    key=keys[1000 - key],
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

@pytest.fixture()
def commit_details_imported_payload(import_commit_details_fixture):
    keys = import_commit_details_fixture
    payload = dict(
        organization_key=rails_organization_key,
        repository_key=rails_repository_key,
        repository_name='rails',
        commit_details=[
            dict(
                source_commit_id=f"{key}",
                key=keys[1000 - key].hex,
                parents=['99', '100'],
                stats=dict(
                    files=1,
                    lines=10,
                    insertions=8,
                    deletions=2
                ),
                source_files=[
                    dict(
                        key=uuid.uuid4().hex,
                        path='test/',
                        name='files1.txt',
                        file_type='txt',
                        version_count=1,
                        is_deleted=False,
                        action='A',
                        stats={"lines": 2, "insertions": 2, "deletions": 0}
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        path='test/',
                        name='files2.py',
                        file_type='py',
                        version_count=1,
                        is_deleted=False,
                        action='A',
                        stats={"lines": 2, "insertions": 2, "deletions": 0}
                    )
                ]
            )
            for key in range(1000, 1010)
        ]
    )
    yield payload


