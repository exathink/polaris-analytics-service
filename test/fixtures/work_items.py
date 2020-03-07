# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid

# work_item_sources
rails_work_items_source_key = uuid.uuid4()
rails_work_items_source_id = 1000
polaris_work_items_source_key = uuid.uuid4()
empty_work_items_source_key = uuid.uuid4()


from test.fixtures.repo_org import *

from polaris.common import db
from polaris.analytics.db.model import WorkItemsSource
from datetime import datetime


def work_items_common():
    return dict(
        work_item_type='issue',
        is_bug=True,
        url='http://foo.com',
        tags=['ares2'],
        description='An issue here',
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        state='open',
        source_id=str(uuid.uuid4())
    )


def work_item_source_common():
    return dict(
        key=rails_work_items_source_key.hex,
        name='Rails Project',
        integration_type='github',
        work_items_source_type='repository_issues',
        commit_mapping_scope='organization',
        commit_mapping_scope_key=rails_organization_key,
        source_id=str(uuid.uuid4())
    )


@pytest.yield_fixture()
def work_items_setup(setup_repo_org):
    _, organization_id = setup_repo_org
    with db.orm_session() as session:
        work_items_source = WorkItemsSource(
                organization_id=organization_id,
                organization_key=rails_organization_key,
                **work_item_source_common()
            )
        work_items_source.init_state_map()
        session.add(
            work_items_source
        )

    yield rails_organization_key, rails_work_items_source_key

    db.connection().execute("delete from analytics.work_item_delivery_cycle_durations")
    db.connection().execute("delete from analytics.work_item_delivery_cycles")
    db.connection().execute("delete from analytics.work_items_source_state_map")
    db.connection().execute("delete from analytics.work_item_state_transitions")
    db.connection().execute("delete from analytics.work_items")
    db.connection().execute("delete from analytics.work_items_sources")


@pytest.yield_fixture
def update_work_items_setup(work_items_setup):
    organization_key, work_items_source_key = work_items_setup
    work_items = [
        dict(
            key=uuid.uuid4().hex,
            name=str(i),
            display_id=str(i),
            **work_items_common()
        )
        for i in range(0, 2)

    ]
    with db.orm_session() as session:
        work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
        work_items_source.work_items.extend([
            model.WorkItem(**work_item)
            for work_item in work_items
        ])

    yield organization_key, work_items_source_key, work_items