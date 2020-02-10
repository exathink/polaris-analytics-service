# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime
from test.constants import *
from test.fixtures.repo_org import *

from polaris.analytics.db import impl
from polaris.common import db

commit_common_fields = dict(
    commit_date=datetime.utcnow(),
    commit_date_tz_offset=0,
    author_date=datetime.utcnow(),
    author_date_tz_offset=0,
    created_at=datetime.utcnow(),
    commit_message='a change'

)

joe_alt_contributor_key = uuid.uuid4().hex

"""
Common setup for contributor updates. We create commits with three contributor aliases. 
Assign one alias to committers and assign the other two alias to authors.
Then we test by reassigning contributor_aliases 
"""
@pytest.fixture
def setup_commits_for_contributor_updates(setup_repo_org):
    with db.create_session() as session:
        impl.import_new_commits(
            session,
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            new_commits=[
                dict(
                    source_commit_id='XXXX',
                    key=uuid.uuid4().hex,
                    author_alias_key=joe_contributor_key,
                    committer_alias_key=billy_contributor_key,
                    **commit_common_fields
                ),
                dict(
                    source_commit_id='YYYY',
                    key=uuid.uuid4().hex,
                    author_alias_key=joe_alt_contributor_key,
                    committer_alias_key=billy_contributor_key,
                    **commit_common_fields
                ),

            ],
            new_contributors=[
                dict(
                    name='Joe Blow',
                    key=joe_contributor_key,
                    alias='joe@blow.com'
                ),
                dict(
                    name='Joe G. Blow',
                    key=joe_alt_contributor_key,
                    alias='joe-blow@aol.com'
                ),
                dict(
                    name='Billy Bob',
                    key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )