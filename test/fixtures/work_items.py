# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid

# work_item_sources
rails_work_items_source_key = uuid.uuid4()
polaris_work_items_source_key = uuid.uuid4()
empty_work_items_source_key = uuid.uuid4()


from test.fixtures.repo_org import *

from polaris.common import db
from polaris.analytics.db.model import WorkItemsSource
from datetime import datetime


def work_items_common():
    return dict(
        is_bug=True,
        url='http://foo.com',
        tags=['ares2'],
        integration_type='github',
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        state='open'
    )


def work_item_source_common():
    return dict(
        key=rails_work_items_source_key.hex,
        name='Rails Project',
        integration_type='github',
        commit_mapping_scope='organization',
        commit_mapping_scope_key=rails_organization_key
    )


@pytest.yield_fixture()
def work_items_setup(setup_repo_org):
    _, organization_id = setup_repo_org
    with db.orm_session() as session:
        session.add(
            WorkItemsSource(
                organization_id=organization_id,
                organization_key=rails_organization_key,
                **work_item_source_common()
            )
        )

    yield rails_organization_key, rails_work_items_source_key

    db.connection().execute("delete from analytics.work_items")
    db.connection().execute("delete from analytics.work_items_sources")