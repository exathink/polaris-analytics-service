# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import uuid
from test.fixtures.work_items import *
from polaris.analytics.db import api, model
from polaris.common import db
from polaris.utils.collections import dict_merge
class TestRegisterWorkItemsSource:

    def it_registers_a_work_item_source(self, setup_org):
        organization = setup_org
        source_key = uuid.uuid4()
        result = api.register_work_items_source(
            organization.key,
            dict(
                key=source_key,
                name='rails',
                integration_type='github',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=organization.key
        ))

        assert result['created']
        assert db.connection().execute(
            f"select count(id) from analytics.work_items_sources where key='{source_key}'"
        ).scalar() == 1

    def it_is_idempotent(self, setup_org):
        organization = setup_org
        source_key = uuid.uuid4()
        # call once
        api.register_work_items_source(
            organization.key,
            dict(
                key=source_key,
                name='rails',
                integration_type='github',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=organization.key
            ))
        # call again
        result = api.register_work_items_source(
            organization.key,
            dict(
                key=source_key,
                name='rails',
                integration_type='github',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=organization.key
            ))
        assert not result['created']
        assert db.connection().execute(
            f"select count(id) from analytics.work_items_sources where key='{source_key}'"
        ).scalar() == 1

class TestImportWorkItems:

    def it_imports_new_work_items(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup

        result = api.import_new_work_items(organization_key, work_items_source_key, [
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_common()
            )
            for i in range(0, 10)
        ])
        assert result['success']
        assert db.connection().execute('select count(id) from analytics.work_items').scalar() == 10


    def it_is_idempotent(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_common()
            )
            for i in range(0, 10)
        ]

        api.import_new_work_items(organization_key, work_items_source_key, work_items)
        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)

        assert result['success']
        assert result['insert_count'] == 0
        assert db.connection().execute('select count(id) from analytics.work_items').scalar() == 10


    def it_only_creates_new_items(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_common()
            )
            for i in range(0, 10)
        ]

        api.import_new_work_items(organization_key, work_items_source_key, work_items)
        work_items.append(
            dict(
                key=uuid.uuid4().hex,
                name='new',
                display_id='new',
                **work_items_common()
            )
        )
        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)

        assert result['success']
        assert result['insert_count'] == 1
        assert db.connection().execute('select count(id) from analytics.work_items').scalar() == 11


class TestUpdateWorkItems:

    def it_updates_name(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(name='foo')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute("select count(id) from analytics.work_items where name='foo'").scalar() == 2

    def it_updates_description(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(description='foo')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute("select count(id) from analytics.work_items where description='foo'").scalar() == 2

    def it_updates_url(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(url='foo')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute("select count(id) from analytics.work_items where url='foo'").scalar() == 2

    def it_updates_tags(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(tags=['foo'])
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute("select count(id) from analytics.work_items where tags='{\"foo\"}'").scalar() == 2

    def it_updates_state(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(state='foo')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute("select count(id) from analytics.work_items where state='foo'").scalar() == 2




    def it_saves_the_state_change_histories(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(state='foo')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute("select count(*) from analytics.work_item_state_transitions").scalar() == 2


    def it_returns_state_changes(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(state='foo')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert result['state_changes']
        assert len(result['state_changes']) == 2
        assert all(
            map(
                lambda change: change['previous_state'] == 'open' and change['state'] == 'foo',
                result['state_changes']
            )
        )

class TestStateTransitionSequence:

    def it_saves_an_initial_state_transition_for_new_items(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_item = dict(
                key=uuid.uuid4().hex,
                name='bar',
                display_id='1000',
                **work_items_common()
            )
        result = api.import_new_work_items(organization_key, work_items_source_key, [
            work_item
        ])
        assert result['success']
        assert db.row_proxy_to_dict(
            db.connection().execute("select seq_no, previous_state, state, created_at from analytics.work_item_state_transitions").fetchone()
        ) == dict(
            seq_no=0,
            previous_state=None,
            state=work_item['state'],
            created_at=work_item['updated_at']
        )


    def it_initializes_the_next_state_seq_no_for_the_new_item(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_item_key=uuid.uuid4().hex
        work_item = dict(
                key=work_item_key,
                name='bar',
                display_id='1000',
                **work_items_common()
            )
        result = api.import_new_work_items(organization_key, work_items_source_key, [
            work_item
        ])
        assert result['success']
        assert db.connection().execute(f"select next_state_seq_no from analytics.work_items where key='{work_item_key}'").scalar() == 1



    def it_saves_the_next_state_when_there_is_an_update_with_a_state_change(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_item_key = uuid.uuid4().hex
        work_item = dict(
                key=work_item_key,
                name='bar',
                display_id='1000',
                **work_items_common()
            )
        result = api.import_new_work_items(organization_key, work_items_source_key, [
            work_item
        ])
        assert result['success']
        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(
                    state='closed'
                )
            )
        ])
        assert result['success']
        assert db.row_proxies_to_dict(
            db.connection().execute(
                "select seq_no, previous_state, state, created_at from analytics.work_item_state_transitions order by seq_no").fetchall()
        ) == [
            dict(
                seq_no=0,
                previous_state=None,
                state=work_item['state'],
                created_at=work_item['updated_at']
            ),
            dict(
                seq_no=1,
                previous_state=work_item['state'],
                state='closed',
                created_at=work_item['updated_at']
            )
        ]


    def it_updates_the_next_state_seq_no_after_the_update(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_item_key = uuid.uuid4().hex
        work_item = dict(
                key=work_item_key,
                name='bar',
                display_id='1000',
                **work_items_common()
            )
        result = api.import_new_work_items(organization_key, work_items_source_key, [
            work_item
        ])
        assert result['success']
        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(
                    state='closed'
                )
            )
        ])
        assert result['success']
        assert db.connection().execute(
            f"select next_state_seq_no from analytics.work_items where key='{work_item_key}'").scalar() == 2


    def it_saves_the_next_state_correctly_after_a_subsequent_update(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_item_key = uuid.uuid4().hex
        work_item = dict(
                key=work_item_key,
                name='bar',
                display_id='1000',
                **work_items_common()
            )
        result = api.import_new_work_items(organization_key, work_items_source_key, [
            work_item
        ])
        assert result['success']
        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(
                    state='closed'
                )
            )
        ])
        assert result['success']

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(
                    state='delivered'
                )
            )
        ])
        assert result['success']

        assert db.row_proxies_to_dict(
            db.connection().execute(
                "select seq_no, previous_state, state, created_at from analytics.work_item_state_transitions order by seq_no").fetchall()
        ) == [
            dict(
                seq_no=0,
                previous_state=None,
                state=work_item['state'],
                created_at=work_item['updated_at']
            ),
            dict(
                seq_no=1,
                previous_state=work_item['state'],
                state='closed',
                created_at=work_item['updated_at']
            ),
            dict(
                seq_no=2,
                previous_state='closed',
                state='delivered',
                created_at=work_item['updated_at']
            )
        ]

    def it_creates_a_state_transition_only_if_the_state_has_changed(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_item_key = uuid.uuid4().hex
        work_item = dict(
                key=work_item_key,
                name='bar',
                display_id='1000',
                **work_items_common()
            )
        result = api.import_new_work_items(organization_key, work_items_source_key, [
            work_item
        ])
        assert result['success']
        result = api.update_work_items(organization_key, work_items_source_key, [
            work_item
        ])
        assert result['success']
        assert db.row_proxies_to_dict(
            db.connection().execute(
                "select seq_no, previous_state, state, created_at from analytics.work_item_state_transitions order by seq_no").fetchall()
        ) == [
            dict(
                seq_no=0,
                previous_state=None,
                state=work_item['state'],
                created_at=work_item['updated_at']
            )
        ]

    def it_updates_the_next_state_seq_no_only_if_the_state_has_changed(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_item_key=uuid.uuid4().hex
        work_item = dict(
                key=work_item_key,
                name='bar',
                display_id='1000',
                **work_items_common()
            )
        result = api.import_new_work_items(organization_key, work_items_source_key, [
            work_item
        ])
        assert result['success']
        result = api.update_work_items(organization_key, work_items_source_key, [
            work_item
        ])

        assert result['success']
        assert db.connection().execute(f"select next_state_seq_no from analytics.work_items where key='{work_item_key}'").scalar() == 1

