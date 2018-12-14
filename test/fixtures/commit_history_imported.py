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
    db.connection().execute("delete from analytics.contributors")
