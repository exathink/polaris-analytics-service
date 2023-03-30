# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from polaris.analytics.db import api
from polaris.analytics.db.enums import WorkItemsStateType

# work_item_sources
rails_work_items_source_key = uuid.uuid4()
rails_work_items_source_id = 1000
polaris_work_items_source_key = uuid.uuid4()
empty_work_items_source_key = uuid.uuid4()

from test.fixtures.repo_org import *

from polaris.common import db
from polaris.analytics.db.model import WorkItemsSource
from datetime import datetime, timedelta
from polaris.utils.collections import Fixture

def work_items_common():
    return dict(
        work_item_type='issue',
        is_bug=True,
        is_epic=False,
        url='http://foo.com',
        tags=['ares2'],
        description='An issue here',
        created_at=datetime.utcnow() - timedelta(days=7),
        updated_at=datetime.utcnow() - timedelta(days=6),
        state='open',
        source_id=str(uuid.uuid4()),
        parent_id=None
    )


def work_items_closed():
    return dict(
        work_item_type='issue',
        is_bug=True,
        is_epic=False,
        url='http://foo.com',
        tags=['ares2'],
        description='An issue here',
        created_at=datetime.utcnow() - timedelta(days=7),
        updated_at=datetime.utcnow(),
        state='closed',
        source_id=str(uuid.uuid4()),
        parent_id=None
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


@pytest.fixture()
def work_items_setup(setup_repo_org):
    _, organization_id = setup_repo_org
    with db.orm_session() as session:
        work_items_source = WorkItemsSource(
            organization_id=organization_id,
            organization_key=rails_organization_key,
            **work_item_source_common()
        )
        state_map_entries = [
            dict(state='created', state_type=WorkItemsStateType.backlog.value),
            dict(state='open', state_type=WorkItemsStateType.open.value),
            dict(state='wip', state_type=WorkItemsStateType.wip.value),
            dict(state='complete', state_type=WorkItemsStateType.complete.value),
            dict(state='closed', state_type=WorkItemsStateType.closed.value),
            dict(state='done', state_type=WorkItemsStateType.closed.value)
        ]
        work_items_source.init_state_map(state_map_entries)
        session.add(
            work_items_source
        )

    yield rails_organization_key, rails_work_items_source_key

    db.connection().execute("delete from analytics.work_item_delivery_cycle_contributors")
    db.connection().execute("delete from analytics.work_item_delivery_cycle_durations")
    db.connection().execute("delete from analytics.work_item_delivery_cycles")
    db.connection().execute("delete from analytics.work_items_source_state_map")
    db.connection().execute("delete from analytics.work_item_state_transitions")
    db.connection().execute("delete from analytics.work_items")
    db.connection().execute("delete from analytics.work_items_sources")

class WorkItemsTest:

    @pytest.fixture
    def setup(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup

        yield Fixture(
            organization_key=organization_key,
            work_items_source_key=work_items_source_key,
            work_items_common=work_items_common()
        )

@pytest.fixture
def update_work_items_setup(work_items_setup):
    organization_key, work_items_source_key = work_items_setup
    work_items_list = [
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
        work_items = [model.WorkItem(**work_item) for work_item in work_items_list]
        work_items_source.work_items.extend(work_items)
        session.add_all(work_items)
        # Adding state transition from null to created, and from created to state
        seq_1_transition_time = datetime.utcnow() - timedelta(days=5)
        for work_item in work_items:
            # Adding state_transitions
            work_item.state_transitions.extend([
                model.WorkItemStateTransition(
                    work_item_id=work_item.id,
                    seq_no=0,
                    created_at=work_item.created_at,
                    previous_state=None,
                    state='created'),
                model.WorkItemStateTransition(
                    work_item_id=work_item.id,
                    seq_no=1,
                    created_at=seq_1_transition_time,
                    previous_state='created',
                    state=work_item.state)
            ])
            work_item.next_state_seq_no = 2
            work_item.state_type = work_items_source.get_state_type(work_item.state)

            # Adding delivery cycles
            work_item.delivery_cycles.extend([
                model.WorkItemDeliveryCycle(
                    work_items_source_id=work_items_source.id,
                    start_seq_no=0,
                    start_date=work_item.created_at,
                )
            ])
        session.flush()
        for work_item in work_items:
            work_item.current_delivery_cycle_id = max([dc.delivery_cycle_id for dc in work_item.delivery_cycles])

    yield organization_key, work_items_source_key, work_items_list


@pytest.fixture()
def update_closed_work_items_setup(work_items_setup):
    organization_key, work_items_source_key = work_items_setup
    work_items_list = [
        dict(
            key=uuid.uuid4().hex,
            name=str(i),
            display_id=str(i),
            **work_items_closed()
        )
        for i in range(0, 2)
    ]
    with db.orm_session() as session:
        work_items_source = WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
        work_items = [model.WorkItem(**work_item) for work_item in work_items_list]
        work_items_source.work_items.extend(work_items)
        session.add_all(work_items)
        # Adding state transition from null to created, and from created to state
        seq_1_transition_time = datetime.utcnow()
        for work_item in work_items:
            # Adding state_transitions
            work_item.state_transitions.extend([
                model.WorkItemStateTransition(
                    work_item_id=work_item.id,
                    seq_no=0,
                    created_at=work_item.created_at,
                    previous_state=None,
                    state='created'),
                model.WorkItemStateTransition(
                    work_item_id=work_item.id,
                    seq_no=1,
                    created_at=seq_1_transition_time,
                    previous_state='created',
                    state=work_item.state)
            ])
            work_item.next_state_seq_no = 2
            work_item.state_type = work_items_source.get_state_type(work_item.state)

            # Adding delivery cycles
            work_item.delivery_cycles.extend([
                model.WorkItemDeliveryCycle(
                    work_items_source_id=work_items_source.id,
                    start_seq_no=0,
                    end_seq_no=1,
                    start_date=work_item.created_at,
                    end_date=seq_1_transition_time,
                    lead_time=int((seq_1_transition_time - work_item.created_at).total_seconds())
                )
            ])

            # Skipping adding work_item_delivery_cycle_durations as update_work_items api will do upsert

        session.flush()
        for work_item in work_items:
            work_item.current_delivery_cycle_id = max([dc.delivery_cycle_id for dc in work_item.delivery_cycles])

    yield organization_key, work_items_source_key, work_items_list
