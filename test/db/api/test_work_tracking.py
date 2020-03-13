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
from polaris.common.enums import WorkTrackingIntegrationType
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
                work_items_source_type='repository_issues',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=organization.key,
                source_id=rails_work_items_source_id
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
                work_items_source_type='repository_issues',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=organization.key,
                source_id=rails_work_items_source_id
            ))
        # call again
        result = api.register_work_items_source(
            organization.key,
            dict(
                key=source_key,
                name='rails',
                integration_type='github',
                work_items_source_type='repository_issues',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=organization.key,
                source_id=rails_work_items_source_id
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

    def it_updates_state_types_for_added_items(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup

        result = api.import_new_work_items(organization_key, work_items_source_key, [
            dict(
                key=uuid.uuid4().hex,
                name='alpha',
                display_id='alpha',
                work_item_type='issue',
                is_bug=True,
                url='http://foo.com',
                tags=['ares2'],
                description='An issue here',
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                state='open',
                source_id=str(uuid.uuid4())
            ),
            dict(
                key=uuid.uuid4().hex,
                name='beta',
                display_id='beta',
                work_item_type='issue',
                is_bug=True,
                url='http://foo.com',
                tags=['ares2'],
                description='An issue here',
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                state='closed',
                source_id=str(uuid.uuid4())
            )
        ])
        assert result['success']
        # alpha should be open.
        assert db.connection().execute(
            "select name from analytics.work_items where state_type='open'"
        ).scalar() == 'alpha'
        # beta should be closed
        assert db.connection().execute(
            "select name from analytics.work_items where state_type='closed'").scalar() == 'beta'

    def it_updates_completion_dates_for_added_items(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup

        result = api.import_new_work_items(organization_key, work_items_source_key, [
            dict(
                key=uuid.uuid4().hex,
                name='alpha',
                display_id='alpha',
                work_item_type='issue',
                is_bug=True,
                url='http://foo.com',
                tags=['ares2'],
                description='An issue here',
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                state='open',
                source_id=str(uuid.uuid4())
            ),
            dict(
                key=uuid.uuid4().hex,
                name='beta',
                display_id='beta',
                work_item_type='issue',
                is_bug=True,
                url='http://foo.com',
                tags=['ares2'],
                description='An issue here',
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                state='closed',
                source_id=str(uuid.uuid4())
            )
        ])
        assert result['success']
        # alpha should not have a completed_at date since it is open.
        assert db.connection().execute(
            "select completed_at from analytics.work_items where name='alpha'"
        ).scalar() is None
        # beta should have a completed_at date since it is closed
        assert db.connection().execute(
            "select completed_at from analytics.work_items where name='beta'").scalar()


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
        assert db.connection().execute(
            "select count(id) from analytics.work_items where description='foo'").scalar() == 2

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
        assert db.connection().execute(
            "select count(id) from analytics.work_items where tags='{\"foo\"}'").scalar() == 2

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

    def it_updates_state_type(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(state='closed')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute(
            "select count(id) from analytics.work_items where state_type = 'closed'").scalar() == 2

    def it_updates_completed_at(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(state='closed')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute(
            "select count(id) from analytics.work_items where completed_at is not null").scalar() == 2

    def it_resets_completion_date_for_transition_from_complete_back_to_open(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(state='closed')
            )
            for work_item in work_items
        ])
        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(state='open')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute(
            "select count(id) from analytics.work_items where completed_at is not null").scalar() == 0

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
        assert db.row_proxies_to_dict(
            db.connection().execute(
                "select seq_no, previous_state, state, created_at from analytics.work_item_state_transitions").fetchall()
        ) == [
                   dict(
                       seq_no=0,
                       previous_state=None,
                       state='created',
                       created_at=work_item['created_at']
                   ),
                   dict(
                       seq_no=1,
                       previous_state='created',
                       state=work_item['state'],
                       created_at=work_item['updated_at']
                   )
               ]

    def it_initializes_the_next_state_seq_no_for_the_new_item(self, work_items_setup):
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
        assert db.connection().execute(
            f"select next_state_seq_no from analytics.work_items where key='{work_item_key}'").scalar() == 2

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
                       state='created',
                       created_at=work_item['created_at']
                   ),
                   dict(
                       seq_no=1,
                       previous_state='created',
                       state=work_item['state'],
                       created_at=work_item['updated_at']
                   ),
                   dict(
                       seq_no=2,
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
            f"select next_state_seq_no from analytics.work_items where key='{work_item_key}'").scalar() == 3

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
                       state='created',
                       created_at=work_item['created_at']
                   ),
                   dict(
                       seq_no=1,
                       previous_state='created',
                       state='open',
                       created_at=work_item['updated_at']
                   ),
                   dict(
                       seq_no=2,
                       previous_state=work_item['state'],
                       state='closed',
                       created_at=work_item['updated_at']
                   ),
                   dict(
                       seq_no=3,
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
                       state='created',
                       created_at=work_item['created_at']
                   ),
                   dict(
                       seq_no=1,
                       previous_state='created',
                       state=work_item['state'],
                       created_at=work_item['updated_at']
                   )
               ]

    def it_updates_the_next_state_seq_no_only_if_the_state_has_changed(self, work_items_setup):
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
        assert db.connection().execute(
            f"select next_state_seq_no from analytics.work_items where key='{work_item_key}'").scalar() == 2


class TestImportProject:

    def it_imports_a_new_project(self, setup_org):
        organization = setup_org
        organization_key = organization.key
        project_key = uuid.uuid4()
        source_key = uuid.uuid4()

        project_summary = dict(
            key=project_key,
            name='foo',
            organization_key=organization.key,
            work_items_sources=[
                dict(
                    name='a source',
                    key=source_key,
                    integration_type='github',
                    commit_mapping_scope='organization',
                    commit_mapping_scope_key=organization.key,
                    description='A new remote project',
                    work_items_source_type='repository_issues',
                    source_id=str(uuid.uuid4())

                )
            ]
        )

        result = api.import_project(organization_key, project_summary)
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.projects where key='{project_key}'"
        ).scalar() == 1

    def it_creates_work_items_sources_when_they_dont_exist(self, setup_org):
        organization = setup_org
        organization_key = organization.key
        project_key = uuid.uuid4()
        source_key = uuid.uuid4()

        project_summary = dict(
            key=project_key,
            name='foo',
            organization_key=organization.key,
            work_items_sources=[
                dict(
                    name='a source',
                    key=source_key,
                    integration_type='github',
                    commit_mapping_scope='organization',
                    commit_mapping_scope_key=organization.key,
                    description='A new remote project',
                    work_items_source_type='repository_issues',
                    source_id=str(uuid.uuid4())
                )
            ]
        )

        result = api.import_project(organization_key, project_summary)
        assert result['success']
        assert db.connection().execute(
            f"select work_items_sources.key from analytics.work_items_sources inner join analytics.projects"
            f" on projects.id = work_items_sources.project_id where projects.key='{project_key}'"
        ).scalar() == source_key

    def it_initializes_default_state_map_for_new_github_work_item_sources(self, setup_org):
        organization = setup_org
        organization_key = organization.key
        project_key = uuid.uuid4()
        source_key = uuid.uuid4()

        project_summary = dict(
            key=project_key,
            name='foo',
            organization_key=organization.key,
            work_items_sources=[
                dict(
                    name='a source',
                    key=source_key,
                    integration_type=WorkTrackingIntegrationType.github.value,
                    commit_mapping_scope='organization',
                    commit_mapping_scope_key=organization.key,
                    description='A new remote project',
                    work_items_source_type='repository_issues',
                    source_id=str(uuid.uuid4())
                )
            ]
        )

        result = api.import_project(organization_key, project_summary)
        assert result['success']
        assert db.connection().execute(
            f"select count(*) from analytics.work_items_sources"
            f" inner join analytics.projects on projects.id = work_items_sources.project_id"
            f" inner join analytics.work_items_source_state_map on work_items_source_state_map.work_items_source_id = work_items_sources.id"
            f" where projects.key='{project_key}'"
        ).scalar() == 3

    def it_initializes_default_state_map_for_new_pivotal_work_item_sources(self, setup_org):
        organization = setup_org
        organization_key = organization.key
        project_key = uuid.uuid4()
        source_key = uuid.uuid4()

        project_summary = dict(
            key=project_key,
            name='foo',
            organization_key=organization.key,
            work_items_sources=[
                dict(
                    name='a source',
                    key=source_key,
                    integration_type=WorkTrackingIntegrationType.pivotal.value,
                    commit_mapping_scope='organization',
                    commit_mapping_scope_key=organization.key,
                    description='A new remote project',
                    work_items_source_type='repository_issues',
                    source_id=str(uuid.uuid4())
                )
            ]
        )

        result = api.import_project(organization_key, project_summary)
        assert result['success']
        assert db.connection().execute(
            f"select count(*) from analytics.work_items_sources"
            f" inner join analytics.projects on projects.id = work_items_sources.project_id"
            f" inner join analytics.work_items_source_state_map on work_items_source_state_map.work_items_source_id = work_items_sources.id"
            f" where projects.key='{project_key}'"
        ).scalar() == 8

    def it_allows_the_state_map_to_be_reinitialized(self, setup_org):
        organization = setup_org
        organization_key = organization.key
        project_key = uuid.uuid4()
        source_key = uuid.uuid4()

        project_summary = dict(
            key=project_key,
            name='foo',
            organization_key=organization.key,
            work_items_sources=[
                dict(
                    name='a source',
                    key=source_key,
                    integration_type=WorkTrackingIntegrationType.pivotal.value,
                    commit_mapping_scope='organization',
                    commit_mapping_scope_key=organization.key,
                    description='A new remote project',
                    work_items_source_type='repository_issues',
                    source_id=str(uuid.uuid4())
                )
            ]
        )

        result = api.import_project(organization_key, project_summary)
        assert result['success']
        with db.orm_session() as session:
            project = model.Project.find_by_project_key(session, project_key)
            if project is not None:
                work_items_source = project.work_items_sources[0]
                if work_items_source:
                    work_items_source.init_state_map([
                        dict(state='open', state_type='open')
                    ])

        assert db.connection().execute(
            f"select count(*) from analytics.work_items_sources"
            f" inner join analytics.projects on projects.id = work_items_sources.project_id"
            f" inner join analytics.work_items_source_state_map on work_items_source_state_map.work_items_source_id = work_items_sources.id"
            f" where projects.key='{project_key}'"
        ).scalar() == 1

    def it_does_not_initialize_default_state_map_for_new_jira_work_item_sources(self, setup_org):
        organization = setup_org
        organization_key = organization.key
        project_key = uuid.uuid4()
        source_key = uuid.uuid4()

        project_summary = dict(
            key=project_key,
            name='foo',
            organization_key=organization.key,
            work_items_sources=[
                dict(
                    name='a source',
                    key=source_key,
                    integration_type=WorkTrackingIntegrationType.jira.value,
                    commit_mapping_scope='organization',
                    commit_mapping_scope_key=organization.key,
                    description='A new remote project',
                    work_items_source_type='repository_issues',
                    source_id=str(uuid.uuid4())
                )
            ]
        )

        result = api.import_project(organization_key, project_summary)
        assert result['success']
        assert db.connection().execute(
            f"select count(*) from analytics.work_items_sources"
            f" inner join analytics.projects on projects.id = work_items_sources.project_id"
            f" inner join analytics.work_items_source_state_map on work_items_source_state_map.work_items_source_id = work_items_sources.id"
            f" where projects.key='{project_key}'"
        ).scalar() == 0

    def it_returns_new_work_items_sources(self, setup_org):
        organization = setup_org
        organization_key = organization.key
        project_key = uuid.uuid4()
        source_key = uuid.uuid4()

        project_summary = dict(
            key=project_key,
            name='foo',
            organization_key=organization.key,
            work_items_sources=[
                dict(
                    name='a source',
                    key=source_key,
                    integration_type='github',
                    commit_mapping_scope='organization',
                    commit_mapping_scope_key=organization.key,
                    description='A new remote project',
                    work_items_source_type='repository_issues',
                    source_id=str(uuid.uuid4())
                )
            ]
        )

        result = api.import_project(organization_key, project_summary)
        assert result['success']
        assert result['new_work_items_sources'] == 1

    def it_is_idempotent(self, setup_org):
        organization = setup_org
        organization_key = organization.key
        project_key = uuid.uuid4()
        source_key = uuid.uuid4()

        project_summary = dict(
            key=project_key,
            name='foo',
            organization_key=organization.key,
            work_items_sources=[
                dict(
                    name='a source',
                    key=source_key,
                    integration_type='github',
                    commit_mapping_scope='organization',
                    commit_mapping_scope_key=organization.key,
                    description='A new remote project',
                    work_items_source_type='repository_issues',
                    source_id=str(uuid.uuid4())
                )
            ]
        )
        # import once
        api.import_project(organization_key, project_summary)
        # import again
        result = api.import_project(organization_key, project_summary)
        assert result['success']
        assert db.connection().execute(
            f"select count(id) from analytics.projects where key='{project_key}'"
        ).scalar() == 1


class TestWorkItemDeliveryCycles:

    def it_creates_delivery_cycles_for_new_work_items(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_items = []
        work_items.extend([
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_common()
            )
            for i in range(0, 5)]
        )
        work_items.extend([
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_closed()
            )
            for i in range(5, 10)]
        )

        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles').scalar() == 10

    def it_maps_current_delivery_cycle_id_for_new_work_items(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_items = []
        work_items.extend([
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_common()
            )
            for i in range(0, 5)]
        )
        work_items.extend([
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_closed()
            )
            for i in range(5, 10)]
        )

        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        assert result['updated'] == 10
        # we do distinct check here to make sure that we have assigned the 10 new delivery cycles
        # to different work items.
        assert db.connection().execute(
            'select count(DISTINCT current_delivery_cycle_id) from analytics.work_items').scalar() == 10

    def it_only_updates_current_delivery_cycle_id_for_work_items_in_the_current_import_set(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_items = []
        work_items.extend([
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_common()
            )
            for i in range(0, 5)]
        )
        # import the first five
        api.import_new_work_items(organization_key, work_items_source_key, work_items)
        # now import the old items again with the new items
        work_items.extend([
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_closed()
            )
            for i in range(5, 10)]
        )

        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        assert result['updated'] == 5
        assert db.connection().execute(
            'select count(DISTINCT current_delivery_cycle_id) from analytics.work_items').scalar() == 10

    def it_calculates_lead_time_for_work_items_imported_in_closed_state(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_items = []
        work_items.extend([
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_common()
            )
            for i in range(0, 5)]
        )
        work_items.extend([
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_closed()
            )
            for i in range(5, 10)]
        )

        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where lead_time=7').scalar() == 5
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where lead_time is NULL').scalar() == 5
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where end_seq_no is NULL').scalar() == 5
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where end_seq_no=1').scalar() == 5


class TestUpdateWorkItemsDeliveryCycles:

    def it_updates_lead_time_and_end_date_for_closed_work_items(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_items = []
        work_items.extend([
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_common()
            )
            for i in range(0, 5)]
        )
        work_items[0]['created_at'] = datetime.utcnow()-timedelta(days=7)
        api.import_new_work_items(organization_key, work_items_source_key, work_items)
        work_items[0]['state'] = 'closed'
        work_items[0]['updated_at'] = datetime.utcnow()
        result = api.update_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycles \
            where lead_time is not NULL and end_date is not NULL").scalar() == 1


    def it_creates_new_delivery_cycle_when_state_type_changes_from_closed_to_non_closed(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_item_key = uuid.uuid4().hex
        work_items = []
        work_items.extend([
            dict(
                key=work_item_key,
                name=str(i),
                display_id=str(i),
                **work_items_closed()
            )
            for i in range(0, 1)]
        )
        work_items.extend([
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_closed()
            )
            for i in range(1, 2)]
        )
        api.import_new_work_items(organization_key, work_items_source_key, work_items)
        work_items[0]['state'] = 'open'
        work_items[1]['state'] = 'open'
        result = api.update_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles').scalar() == 4
        assert db.connection().execute('select count(DISTINCT current_delivery_cycle_id) from analytics.work_items\
         join analytics.work_item_delivery_cycles on work_items.id=work_item_delivery_cycles.work_item_id \
         where work_items.current_delivery_cycle_id > work_item_delivery_cycles.delivery_cycle_id').scalar() == 2