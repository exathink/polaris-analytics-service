# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.constants import *

from _datetime import datetime
import uuid
import pytest

from polaris.common import db
from polaris.analytics.db import api, model

joe_contributor_key = uuid.uuid4().hex
billy_contributor_key = uuid.uuid4().hex

commit_common_fields = dict(
    commit_date=datetime.utcnow(),
    commit_date_tz_offset=0,
    committer_contributor_key=joe_contributor_key,
    committer_contributor_name='Joe Blow',
    author_date=datetime.utcnow(),
    author_date_tz_offset=0,
    author_contributor_key=billy_contributor_key,
    author_contributor_name='Billy Bob',
    parents=["0000", "0001"],
    stats=dict(
        files=10,
        lines=20,
        insertions=10,
        deletions=10
    ),
    created_at=datetime.utcnow()

)

commit_history_imported_common = dict(
            organization_key=rails_organization_key,
            repository_name='rails',
            repository_key=rails_repository_key,
            branch_info={}

        )

@pytest.yield_fixture
def cleanup(setup_schema):
    yield

    db.connection().execute("delete from analytics.commits")
    db.connection().execute("delete from analytics.contributors")

class TestCommitImport:

    def it_imports_a_single_new_commit_with_new_committer_and_author(self, cleanup):

        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits= [
                dict(
                    commit_key='XXXX',
                    **commit_common_fields
                )
            ],
            new_contributors= [
                dict(
                    name='Joe Blow',
                    contributor_key=joe_contributor_key,
                    alias='joe@blow.com'
                ),
                dict(
                    name='Billy Bob',
                    contributor_key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )

        assert result['success']
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 1
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2


    def it_imports_a_single_new_commit_with_existing_contributors(self, cleanup):

        db.connection().execute(
            model.contributors.insert(
                [
                    dict(
                        name='Joe Blow',
                        key=joe_contributor_key,
                        source_alias='joe@blow.com',
                        source='vcs'
                    ),
                    dict(
                        name='Billy Bob',
                        key=billy_contributor_key,
                        source_alias='billy@bob.com',
                        source='vcs'
                    )
                ]
            )
        )
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits= [
                dict(
                    commit_key='XXXX',
                    **commit_common_fields
                )
            ],
            new_contributors= []
        )

        assert result['success']
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 1
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2

    def it_imports_a_single_new_commit_with_existing_and_new_contributors(self, cleanup):

        db.connection().execute(
            model.contributors.insert(
                [
                    dict(
                        name='Joe Blow',
                        key=joe_contributor_key,
                        source_alias='joe@blow.com',
                        source='vcs'
                    )
                ]
            )
        )
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits= [
                dict(
                    commit_key='XXXX',
                    **commit_common_fields
                )
            ],
            new_contributors= [
                dict(
                    name='Billy Bob',
                    contributor_key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )

        assert result['success']
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 1
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2

    def it_imports_multiple_commit_with_existing_and_new_contributors(self, cleanup):

        db.connection().execute(
            model.contributors.insert(
                [
                    dict(
                        name='Joe Blow',
                        key=joe_contributor_key,
                        source_alias='joe@blow.com',
                        source='vcs'
                    )
                ]
            )
        )
        result = api.import_new_commits(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits= [
                dict(
                    commit_key=f'XXXX-{i}',
                    **commit_common_fields
                )
                for i in range(0,9)
            ],
            new_contributors= [
                dict(
                    name='Billy Bob',
                    contributor_key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )

        assert result['success']
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 9
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2

    def it_is_idempotent(self, cleanup):

        db.connection().execute(
            model.contributors.insert(
                [
                    dict(
                        name='Joe Blow',
                        key=joe_contributor_key,
                        source_alias='joe@blow.com',
                        source='vcs'
                    )
                ]
            )
        )
        args=dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    commit_key=f'XXXX-{i}',
                    **commit_common_fields
                )
                for i in range(0, 9)
            ],
            new_contributors=[
                dict(
                    name='Billy Bob',
                    contributor_key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )
        # first import once
        api.import_new_commits(**args)
        # now import again
        result = api.import_new_commits(**args)

        assert result['success']
        assert db.connection().execute("select count(id) from analytics.commits").scalar() == 9
        assert db.connection().execute("select count(id) from analytics.contributors").scalar() == 2