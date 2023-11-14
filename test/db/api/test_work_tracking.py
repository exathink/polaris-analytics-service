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
from polaris.analytics.db.enums import WorkItemType
from polaris.utils.collections import dict_merge, Fixture


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


class TestImportWorkItems(WorkItemsTest):

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

    def it_imports_all_fields_for_new_item(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        new_work_item = dict(
            key=uuid.uuid4().hex,
            name='Issue 100',
            display_id='100',
            **work_items_common()
        )
        result = api.import_new_work_items(organization_key, work_items_source_key, [new_work_item])
        assert result['success']
        result_set = db.connection().execute('select * from analytics.work_items')
        for row in result_set:
            assert row.name == new_work_item['name']
            assert row.display_id == new_work_item['display_id']
            assert row.work_item_type == new_work_item['work_item_type']
            assert row.is_bug == new_work_item['is_bug']
            assert row.is_epic == new_work_item['is_epic']
            assert row.url == new_work_item['url']
            assert row.tags == new_work_item['tags']
            assert row.description == new_work_item['description']
            assert row.created_at == new_work_item['created_at']
            assert row.updated_at == new_work_item['updated_at']
            assert row.state == new_work_item['state']
            assert row.source_id == new_work_item['source_id']
            assert row.parent_id == new_work_item['parent_id']
            assert row.priority == new_work_item['priority']
            assert row.releases == new_work_item['releases']
            assert row.story_points == new_work_item['story_points']
            assert row.sprints == new_work_item['sprints']
            assert row.flagged == new_work_item['flagged']
            assert row.changelog == new_work_item['changelog']

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

    def it_inserts_only_the_new_items_from_a_batch(self, work_items_setup):
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
                name='New item',
                display_id='11',
                **work_items_common()
            )
        )
        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)

        assert result['success']
        assert result['insert_count'] == 1
        assert db.connection().execute('select count(id) from analytics.work_items').scalar() == 11

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
                is_epic=False,
                url='http://foo.com',
                tags=['ares2'],
                description='An issue here',
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                state='open',
                source_id=str(uuid.uuid4()),
                parent_id=None
            ),
            dict(
                key=uuid.uuid4().hex,
                name='beta',
                display_id='beta',
                work_item_type='issue',
                is_bug=True,
                is_epic=False,
                url='http://foo.com',
                tags=['ares2'],
                description='An issue here',
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                state='closed',
                source_id=str(uuid.uuid4()),
                parent_id=None
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
                is_epic=False,
                url='http://foo.com',
                tags=['ares2'],
                description='An issue here',
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                state='open',
                source_id=str(uuid.uuid4()),
                parent_id=None
            ),
            dict(
                key=uuid.uuid4().hex,
                name='beta',
                display_id='beta',
                work_item_type='issue',
                is_bug=True,
                is_epic=False,
                url='http://foo.com',
                tags=['ares2'],
                description='An issue here',
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                state='closed',
                source_id=str(uuid.uuid4()),
                parent_id=None
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

    def it_updates_parent_id_for_added_items(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        parent_key = uuid.uuid4().hex
        result = api.import_new_work_items(organization_key, work_items_source_key, [
            dict(
                key=parent_key,
                name='alpha',
                display_id='alpha',
                work_item_type='issue',
                is_bug=True,
                is_epic=True,
                url='http://foo.com',
                tags=['ares2'],
                description='An issue here',
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                state='open',
                source_id=str(uuid.uuid4()),
                parent_id=None,
                parent_key=None,
            ),
            dict(
                key=uuid.uuid4().hex,
                name='beta',
                display_id='beta',
                work_item_type='issue',
                is_bug=True,
                is_epic=False,
                url='http://foo.com',
                tags=['ares2'],
                description='An issue here',
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                state='closed',
                source_id=str(uuid.uuid4()),
                parent_id=None,
                parent_key=parent_key
            )
        ])
        assert result['success']
        # Finding epi id
        parent_id = db.connection().execute(
            f"select id from analytics.work_items where key='{parent_key}'"
        ).scalar()
        # Work item should have epic id updated
        assert db.connection().execute(
            f"select count(id) from analytics.work_items where parent_id='{parent_id}'").scalar() == 1

    def it_creates_initial_entry_in_impediment_history_if_flagged(self, work_items_setup):
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

        work_items[1]['state'] = 'foo'

        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)

        assert result['success']

        assert db.connection().execute(
            "select count(*) from analytics.work_items_impediment_history join analytics.work_items on analytics.work_items.id = analytics.work_items_impediment_history.work_item_id where analytics.work_items_impediment_history.state = 'foo' and created = created_at and cleared is null").scalar() == 1

        assert db.connection().execute(
            "select count(*) from analytics.work_items_impediment_history join analytics.work_items on analytics.work_items.id = analytics.work_items_impediment_history.work_item_id where analytics.work_items_impediment_history.state = 'open' and created = created_at and cleared is null").scalar() == 1

    def it_does_not_create_entry_in_impediment_history_if_no_flag(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_items = [dict(
            key=uuid.uuid4().hex,
            name='flag test',
            display_id='flag test',
            **work_items_common()
        )
        ]
        work_items[0]['flagged'] = False

        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)

        assert result['success']

        assert db.connection().execute(
            'select count(work_item_id) from analytics.work_items_impediment_history').scalar() == 0

    def it_creates_state_transitions_entry_if_changelog_is_not_None(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_items = [dict(
            key=uuid.uuid4().hex,
            name='changelog test',
            display_id='changelog test',
            **work_items_common()
        )
        ]

        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)

        assert result['success']

        assert db.connection().execute(
            'select count(work_item_id) from analytics.work_item_state_transitions').scalar() == 3

    def it_does_not_create_state_transitions_entry_if_changelog_is_empty(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_items = [dict(
            key=uuid.uuid4().hex,
            name='changelog test',
            display_id='changelog test',
            **work_items_common()
        )
        ]
        work_items[0]['changelog'] = None

        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)

        assert result['success']

        assert db.connection().execute(
            'select count(work_item_id) from analytics.work_item_state_transitions').scalar() == 3

    def it_creates_state_transitions_entry_if_there_are_multiple_changelogs(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_items = [dict(
            key=uuid.uuid4().hex,
            name='changelog test',
            display_id='changelog test',
            **work_items_common()
        )
        ]

        work_items[0]['changelog'] = [{'created': '2023-11-08T16:55:00.575-0600',
                                       'previous_state': 'To Do',
                                       'seq_no': 2,
                                       'state': 'In Progress'},
                                      {'created': '2023-11-09T16:55:00.575-0600',
                                       'previous_state': 'In Progress',
                                       'seq_no': 3,
                                       'state': 'Done'}

                                      ]

        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)

        assert result['success']

        assert db.connection().execute(
            'select count(work_item_id) from analytics.work_item_state_transitions').scalar() == 4

    class TestMultipleSourceImport:

        @pytest.fixture
        def setup(self, setup):
            fixture = setup
            with db.orm_session() as session:
                organization = model.Organization.find_by_organization_key(session, fixture.organization_key)
                cross_project_key = uuid.uuid4()
                cross_project = create_work_items_source(organization.id, work_items_source_key=cross_project_key)
                session.add(cross_project)

            yield Fixture(
                parent=fixture,
                cross_project_key=cross_project_key
            )

        def it_imports_work_items_from_different_work_items_sources(self, setup):
            fixture = setup
            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            cross_project_key = fixture.cross_project_key

            result = api.import_new_work_items(organization_key, work_items_source_key, [
                dict(
                    key=uuid.uuid4().hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    **work_items_common()
                ),

            ])
            assert not result.get('exception')
            assert db.connection().execute('select count(id) from analytics.work_items').scalar() == 2
            with db.orm_session() as session:
                cross_project = model.WorkItemsSource.find_by_work_items_source_key(session, cross_project_key)
                assert len(cross_project.work_items) == 1

        def it_imports_using_the_primary_work_item_source_key_if_one_is_not_provided_in_the_summary(self, setup):
            fixture = setup
            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            cross_project_key = fixture.cross_project_key

            result = api.import_new_work_items(organization_key, work_items_source_key, [
                dict(
                    key=uuid.uuid4().hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=cross_project_key,
                    **work_items_common()
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name="10002",
                    display_id="10002",
                    # we are not providing a work item source key here. Should use primary
                    **work_items_common()
                ),

            ])
            assert result['success']
            assert db.connection().execute('select count(id) from analytics.work_items').scalar() == 2
            with db.orm_session() as session:
                cross_project = model.WorkItemsSource.find_by_work_items_source_key(session, cross_project_key)
                assert len(cross_project.work_items) == 1

                primary = model.WorkItemsSource.find_by_work_items_source_key(session, work_items_source_key)
                assert len(primary.work_items) == 1

        def it_resolves_parent_keys_across_work_items_sources_when_parent_precedes_child(self, setup):
            fixture = setup
            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            cross_project_key = fixture.cross_project_key

            item_keys = [uuid.uuid4(), uuid.uuid4()]
            result = api.import_new_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[0].hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[1].hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                ),

            ])
            assert result['success']
            assert db.connection().execute('select count(id) from analytics.work_items').scalar() == 2
            with db.orm_session() as session:
                child = model.WorkItem.find_by_work_item_key(session, item_keys[1])
                assert child.parent is not None
                assert child.parent.key == item_keys[0]

        def it_resolves_parent_keys_across_work_items_sources_when_child_precedes_parent(self, setup):
            fixture = setup
            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            cross_project_key = fixture.cross_project_key

            item_keys = [uuid.uuid4(), uuid.uuid4()]
            result = api.import_new_work_items(organization_key, work_items_source_key, [

                dict(
                    key=item_keys[1].hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[0].hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                )

            ])
            assert result['success']
            assert db.connection().execute('select count(id) from analytics.work_items').scalar() == 2
            with db.orm_session() as session:
                child = model.WorkItem.find_by_work_item_key(session, item_keys[1])
                assert child.parent is not None
                assert child.parent.key == item_keys[0]

        def it_resolves_parent_keys_across_work_items_sources_when_parent_arrives_after_child(self, setup):
            fixture = setup
            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            cross_project_key = fixture.cross_project_key

            item_keys = [uuid.uuid4(), uuid.uuid4()]

            api.import_new_work_items(organization_key, work_items_source_key, [

                dict(
                    key=item_keys[1].hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                ),

            ])
            result = api.import_new_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[0].hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                )

            ])
            assert result['success']
            assert db.connection().execute('select count(id) from analytics.work_items').scalar() == 2
            with db.orm_session() as session:
                child = model.WorkItem.find_by_work_item_key(session, item_keys[1])
                assert child.parent is not None
                assert child.parent.key == item_keys[0]

        def it_resolves_parent_keys_across_work_items_sources_when_parent_arrives_before_child(self, setup):
            fixture = setup
            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            cross_project_key = fixture.cross_project_key

            item_keys = [uuid.uuid4(), uuid.uuid4()]

            api.import_new_work_items(organization_key, work_items_source_key, [

                dict(
                    key=item_keys[0].hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    parent_key=None,
                    **work_items_common()
                ),

            ])
            result = api.import_new_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[1].hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                )

            ])
            assert result['success']
            assert db.connection().execute('select count(id) from analytics.work_items').scalar() == 2
            with db.orm_session() as session:
                child = model.WorkItem.find_by_work_item_key(session, item_keys[1])
                assert child.parent is not None
                assert child.parent.key == item_keys[0]


class TestUpdateWorkItems(WorkItemsTest):

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

    def it_updates_work_item_type(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(work_item_type='enhancement')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute(
            "select count(id) from analytics.work_items where work_item_type = 'enhancement'").scalar() == 2

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

    def it_updates_priority(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        new_work_item = dict(work_items[0])
        new_work_item['priority'] = 'High'
        result = api.update_work_items(organization_key, work_items_source_key,
                                       [new_work_item
                                        ])
        assert result['success']
        assert db.connection().execute(
            "select count(id) from analytics.work_items where priority='High'").scalar() == 1

    def it_updates_releases(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        new_work_item = dict(work_items[0])
        new_work_item['releases'] = ['{}']
        result = api.update_work_items(organization_key, work_items_source_key,
                                       [new_work_item
                                        ])
        assert result['success']
        with db.orm_session() as session:
            # this tests the single label case in the mapping
            work_item = model.WorkItem.find_by_work_item_key(session, new_work_item['key'])
            assert work_item.releases == ['{}']

    def it_updates_story_points(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        new_work_item = dict(work_items[0])
        new_work_item['story_points'] = 99
        result = api.update_work_items(organization_key, work_items_source_key,
                                       [new_work_item
                                        ])
        assert result['success']
        assert db.connection().execute(
            "select count(id) from analytics.work_items where story_points=99").scalar() == 1

    def it_updates_sprints(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        new_work_item = dict(work_items[0])
        new_work_item['sprints'] = ['Sprint X']
        result = api.update_work_items(organization_key, work_items_source_key,
                                       [new_work_item
                                        ])
        assert result['success']
        assert db.connection().execute(
            "select count(id) from analytics.work_items where sprints = '{Sprint X}'").scalar() == 1

    def it_updates_flagged(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        new_work_item = dict(work_items[0])
        new_work_item['flagged'] = False
        result = api.update_work_items(organization_key, work_items_source_key,
                                       [new_work_item
                                        ])
        assert result['success']
        assert db.connection().execute(
            "select count(id) from analytics.work_items where flagged=False").scalar() == 1

    def it_updates_changelog(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        new_work_item = dict(work_items[0])
        new_work_item['changelog'] = [{'created': '2023-11-09T16:55:00.575-0600',
                                       'previous_state': 'In Progress',
                                       'seq_no': 1,
                                       'state': 'Done'}
                                      ]
        result = api.update_work_items(organization_key, work_items_source_key,
                                       [new_work_item
                                        ])
        assert result['success']
        work_item_display_id = work_items[0]['display_id']
        assert db.connection().execute(
            f"select changelog from analytics.work_items where display_id='{work_item_display_id}'").scalar() == [
                   {'created': '2023-11-09T16:55:00.575-0600',
                    'previous_state': 'In Progress',
                    'seq_no': 1,
                    'state': 'Done'}
               ]

    def it_does_not_update_completed_at_when_state_transitions_from_closed_to_closed(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(state='closed')
            )
            for work_item in work_items[:1]
        ])
        assert result['success']
        completed_at = db.connection().execute(
            "select completed_at from analytics.work_items where completed_at is not null").fetchall()[0]
        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(state='done')
            )
            for work_item in work_items
        ])
        assert result['success']
        new_completed_at = db.connection().execute(
            "select completed_at from analytics.work_items where completed_at is not null").fetchall()[0]
        assert new_completed_at == completed_at

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
        assert db.connection().execute("select count(*) from analytics.work_item_state_transitions").scalar() == 6

    def it_updates_cleared_date_when_state_changes(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        # Test data has one flagged and on unflagged work item

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(state='foo')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute(
            "select count(*) from analytics.work_items_impediment_history join analytics.work_items on analytics.work_items.id = analytics.work_items_impediment_history.work_item_id where cleared = updated_at").scalar() == 1

    def it_updates_cleared_date_when_flagged_changed_from_true_to_false(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        # Test data has one flagged and on unflagged work item
        assert db.connection().execute(
            "select count(*) from analytics.work_items_impediment_history where cleared is not null").scalar() == 0

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(flagged=False)
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute(
            "select count(*) from analytics.work_items_impediment_history join analytics.work_items on analytics.work_items.id = analytics.work_items_impediment_history.work_item_id where cleared = updated_at").scalar() == 1

    def it_inserts_row_into_impediment_history_when_state_changed_and_flagged_is_true(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        # Test data has one flagged and on unflagged work item
        assert db.connection().execute("select count(*) from analytics.work_items_impediment_history").scalar() == 1

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(state='foo')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute(
            "select count(*) from analytics.work_items_impediment_history join analytics.work_items on analytics.work_items.id = analytics.work_items_impediment_history.work_item_id where cleared is null and analytics.work_items_impediment_history.state = 'foo' ").scalar() == 1

    def it_inserts_row_into_impediment_history_when_flagged_changed_to_true(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        # Test data has one flagged and one unflagged work item
        assert db.connection().execute("select count(*) from analytics.work_items_impediment_history").scalar() == 1

        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(flagged=True)
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute(
            "select count(*) from analytics.work_items_impediment_history where analytics.work_items_impediment_history.seq_no = 0").scalar() == 2

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

    def it_updates_parent_id(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        work_items[0]['is_epic'] = True
        work_items[0]['parent_key'] = None
        parent_key = work_items[0]['key']
        work_items[1]['parent_key'] = parent_key
        result = api.update_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        parent_id = db.connection().execute(
            f"select id from analytics.work_items where is_epic=TRUE and key='{parent_key}'").scalar()
        assert db.connection().execute(
            f"select count(id) from analytics.work_items where parent_id='{parent_id}'").scalar() == 1

    def it_returns_new_work_item_keys(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        work_items[0]['key'] = str(uuid.uuid4())
        result = api.update_work_items(organization_key, work_items_source_key, [
            dict_merge(
                work_item,
                dict(name='foo')
            )
            for work_item in work_items
        ])
        assert result['success']
        assert db.connection().execute("select count(id) from analytics.work_items where name='foo'").scalar() == 1
        assert len(result['new_work_items']) == 1
        assert result['new_work_items'][0] == work_items[0]['key']

    class TestMultipleSourceUpdate:

        @pytest.fixture
        def setup(self, setup):
            fixture = setup
            with db.orm_session() as session:
                organization = model.Organization.find_by_organization_key(session, fixture.organization_key)
                cross_project_key = uuid.uuid4()
                cross_project = create_work_items_source(organization.id, work_items_source_key=cross_project_key)
                session.add(cross_project)

            yield Fixture(
                parent=fixture,
                cross_project_key=cross_project_key
            )

        def it_updates_work_items_in_different_work_items_sources(self, setup):
            fixture = setup
            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            cross_project_key = fixture.cross_project_key

            # first import work items
            item_keys = [uuid.uuid4().hex, uuid.uuid4().hex]
            result = api.import_new_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[0],
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[1],
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    **work_items_common()
                ),

            ])
            assert result['success']

            result = api.update_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[0],
                    name="10001-1",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[1],
                    name="10002-1",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    **work_items_common()
                ),

            ])
            assert result['success']
            assert result['update_count'] == 2
            assert len(result['new_work_items']) == 0

            assert db.connection().execute(
                f"select name from analytics.work_items where display_id='10001'").scalar() == "10001-1"
            assert db.connection().execute(
                f"select name from analytics.work_items where display_id='10002'").scalar() == "10002-1"

        def it_updates_parents_across_work_items_sources_from_cross_to_same(self, setup):
            fixture = setup
            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            cross_project_key = fixture.cross_project_key

            # first import work items
            item_keys = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
            result = api.import_new_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[0].hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[1].hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[2].hex,
                    name="10003",
                    display_id="10003",
                    work_items_source_key=cross_project_key,
                    parent_key=item_keys[1].hex,
                    **work_items_common()
                ),

            ])
            assert result['success']

            result = api.update_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[0].hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[1].hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[2].hex,
                    name="10003",
                    display_id="10003",
                    work_items_source_key=cross_project_key,
                    # change this parent
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                )

            ])
            assert result['success']
            assert result['update_count'] == 3
            assert len(result['new_work_items']) == 0

            with db.orm_session() as session:
                child = model.WorkItem.find_by_work_item_key(session, item_keys[2])
                assert child.parent is not None
                assert child.parent.key == item_keys[0]

        def it_updates_parents_across_work_items_sources_from_cross_to_same(self, setup):
            fixture = setup
            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            cross_project_key = fixture.cross_project_key

            # first import work items
            item_keys = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
            result = api.import_new_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[0].hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[1].hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[2].hex,
                    name="10003",
                    display_id="10003",
                    work_items_source_key=cross_project_key,
                    parent_key=item_keys[1].hex,
                    **work_items_common()
                ),

            ])
            assert result['success']

            result = api.update_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[0].hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[1].hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[2].hex,
                    name="10003",
                    display_id="10003",
                    work_items_source_key=cross_project_key,
                    # change this parent
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                )

            ])
            assert result['success']
            assert result['update_count'] == 3
            assert len(result['new_work_items']) == 0

            with db.orm_session() as session:
                child = model.WorkItem.find_by_work_item_key(session, item_keys[2])
                assert child.parent is not None
                assert child.parent.key == item_keys[0]

        def it_updates_parents_to_null(self, setup):
            fixture = setup
            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            cross_project_key = fixture.cross_project_key

            # first import work items
            item_keys = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
            result = api.import_new_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[0].hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[1].hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                ),

            ])
            assert result['success']

            with db.orm_session() as session:
                child = model.WorkItem.find_by_work_item_key(session, item_keys[1])
                assert child.parent is not None

            result = api.update_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[0].hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[1].hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    parent_key=None,
                    **work_items_common()
                )

            ])
            assert result['success']
            assert result['update_count'] == 2
            assert len(result['new_work_items']) == 0

            with db.orm_session() as session:
                child = model.WorkItem.find_by_work_item_key(session, item_keys[1])
                assert child.parent is None

        def it_does_not_add_new_work_items_found_during_updates(self, setup):
            fixture = setup
            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            cross_project_key = fixture.cross_project_key

            # first import work items
            item_keys = [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()]
            result = api.import_new_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[0].hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[1].hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                )
            ])
            assert result['success']

            result = api.update_work_items(organization_key, work_items_source_key, [
                dict(
                    key=item_keys[0].hex,
                    name="10001",
                    display_id="10001",
                    work_items_source_key=work_items_source_key,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[1].hex,
                    name="10002",
                    display_id="10002",
                    work_items_source_key=cross_project_key,
                    parent_key=item_keys[0].hex,
                    **work_items_common()
                ),
                dict(
                    key=item_keys[2].hex,
                    name="10003",
                    display_id="10003",
                    work_items_source_key=cross_project_key,
                    # change this parent from cross parent back to same work item source
                    parent_key=item_keys[1].hex,
                    **work_items_common()
                )

            ])
            assert result['success']
            assert result['update_count'] == 2
            with db.orm_session() as session:
                # new items found during update are not added.
                child = model.WorkItem.find_by_work_item_key(session, item_keys[2])
                assert child is None

            assert len(result['new_work_items']) == 1


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
                        dict(state='created', state_type='open'),
                        dict(state='open', state_type='open')
                    ])

        assert db.connection().execute(
            f"select count(*) from analytics.work_items_sources"
            f" inner join analytics.projects on projects.id = work_items_sources.project_id"
            f" inner join analytics.work_items_source_state_map on work_items_source_state_map.work_items_source_id = work_items_sources.id"
            f" where projects.key='{project_key}'"
        ).scalar() == 2

    def it_interpolates_a_state_map_value_for_created_if_it_is_not_present_in_the_input(self, setup_org):
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
        ).scalar() == 2

        assert db.connection().execute(
            f"select count(*) from analytics.work_items_sources"
            f" inner join analytics.projects on projects.id = work_items_sources.project_id"
            f" inner join analytics.work_items_source_state_map on work_items_source_state_map.work_items_source_id = work_items_sources.id"
            f" where projects.key='{project_key}' and work_items_source_state_map.state='created'"
        ).scalar() == 1

    def it_does_not_initialize_default_state_map_for_new_jira_work_item_sources_only_adds_created_state(self,
                                                                                                        setup_org):
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
        ).scalar() == 1

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
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where lead_time is not NULL').scalar() == 5
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where lead_time is NULL').scalar() == 5
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where end_seq_no is NULL').scalar() == 5
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where end_seq_no=1').scalar() == 5

    def it_does_not_set_cycle_time_for_newly_imported_work_items(self, work_items_setup):
        # Cycle time, like lead time is calculated only for closed work items
        # But cycle time cannot be calculated for work items imported in closed state
        # as there is no knowledge of transitions in open, wip or complete states
        # so it will be null only, even when lead time is not null
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
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where lead_time is not NULL and cycle_time is NULL').scalar() == 5
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where lead_time is NULL and cycle_time is NULL').scalar() == 5

    def it_does_not_set_spec_cycle_time_for_newly_imported_work_items(self, work_items_setup):
        # Like cycle time, spec cycle time cannot be calculated for work items imported in closed state
        # as there is no knowledge of transitions in open, wip or complete states
        # so it will be null only, even when lead time is not null. Also at the time a work item is imported,
        # no commit related information is available for the work item, so spec cycle time will likewise be null
        # for these work items. Best case is that when commits come in, these will update the spec cycle time
        # for this work items where we could not determine cycle time at import.

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
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where lead_time is not NULL and spec_cycle_time is NULL').scalar() == 5
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles where lead_time is NULL and spec_cycle_time is NULL').scalar() == 5


class TestUpdateWorkItemsDeliveryCycles:

    def it_updates_lead_time_and_end_date_for_closed_work_items(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_work_items_setup
        work_items_list[0]['state'] = 'closed'
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list)
        assert result['success']
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycles \
            where lead_time is not NULL and end_date is not NULL").scalar() == 1

    def it_creates_new_delivery_cycle_when_state_type_changes_from_closed_to_non_closed(self,
                                                                                        update_closed_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_closed_work_items_setup
        work_items_list[0]['state'] = 'open'
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list)
        assert result['success']
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles').scalar() == 3
        assert db.connection().execute('select count(DISTINCT current_delivery_cycle_id) from analytics.work_items\
         join analytics.work_item_delivery_cycles on work_items.id=work_item_delivery_cycles.work_item_id \
         where work_items.current_delivery_cycle_id > work_item_delivery_cycles.delivery_cycle_id').scalar() == 1

    def it_does_not_create_new_delivery_cycle_when_state_type_changes_from_closed_to_a_non_mapped_state(self,
                                                                                                        update_closed_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_closed_work_items_setup
        work_items_list[0]['state'] = 'doing'
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list)
        assert result['success']
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles').scalar() == 2
        assert db.connection().execute('select count(DISTINCT current_delivery_cycle_id) from analytics.work_items\
         join analytics.work_item_delivery_cycles on work_items.id=work_item_delivery_cycles.work_item_id \
         where work_items.current_delivery_cycle_id > work_item_delivery_cycles.delivery_cycle_id').scalar() == 0

    def it_does_not_create_new_or_change_old_delivery_cycle_when_state_type_changes_from_closed_to_another_closed_state(
            self,
            update_closed_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_closed_work_items_setup
        work_items_list[1]['state'] = 'done'
        end_seq_no, end_date, lead_time = db.connection().execute(f"select end_seq_no, end_date, lead_time \
            from analytics.work_item_delivery_cycles join analytics.work_items \
            on work_items.id=work_item_delivery_cycles.work_item_id where work_items.key='{work_items_list[1]['key']}'").fetchall()[
            0]
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list[1:])
        assert result['success']
        assert db.connection().execute(
            'select count(delivery_cycle_id) from analytics.work_item_delivery_cycles').scalar() == 2
        new_end_seq_no, new_end_date, new_lead_time = db.connection().execute(f"select end_seq_no, end_date, lead_time \
            from analytics.work_item_delivery_cycles join analytics.work_items \
            on work_items.id=work_item_delivery_cycles.work_item_id where work_items.key='{work_items_list[1]['key']}'").fetchall()[
            0]
        assert end_seq_no == new_end_seq_no
        assert end_date == new_end_date
        assert lead_time == new_lead_time

    def it_sets_the_current_delivery_cycle_of_reopened_items_to_the_new_delivery_cycle(self,
                                                                                       update_closed_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_closed_work_items_setup
        # re-open closed item 1 day after close. Original cycle lead time was 7 days
        work_items_list[0]['updated_at'] = work_items_list[0]['updated_at'] + timedelta(days=1)
        work_items_list[0]['state'] = 'open'
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list)
        assert result['success']
        with db.orm_session() as session:
            work_item = model.WorkItem.find_by_work_item_key(session, work_items_list[0]['key'])
            current_delivery_cycle = work_item.current_delivery_cycle

            assert current_delivery_cycle.start_date == work_items_list[0]['updated_at']
            assert current_delivery_cycle.end_date is None
            assert current_delivery_cycle.lead_time is None

    def it_updates_the_stats_on_new_delivery_cycle_when_the_reopened_item_is_closed(self,
                                                                                    update_closed_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_closed_work_items_setup
        # re-open closed item 1 day after close. Original lead time was 7 days
        work_items_list[0]['updated_at'] = work_items_list[0]['updated_at'] + timedelta(days=1)
        work_items_list[0]['state'] = 'open'
        api.update_work_items(organization_key, work_items_source_key, work_items_list)

        # close it again 2 days after re-open
        work_items_list[0]['updated_at'] = work_items_list[0]['updated_at'] + timedelta(days=2)
        work_items_list[0]['state'] = 'closed'
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list)

        assert result['success']
        with db.orm_session() as session:
            work_item = model.WorkItem.find_by_work_item_key(session, work_items_list[0]['key'])
            delivery_cycles = work_item.delivery_cycles
            assert len(delivery_cycles) == 2
            assert set([int(cycle.lead_time / (3600 * 24)) for cycle in delivery_cycles]) == {7, 2}
            assert set([(cycle.start_seq_no, cycle.end_seq_no) for cycle in delivery_cycles]) == {(0, 1), (2, 3)}

    def it_updates_the_durations_for_the_new_delivery_cycle_when_the_reopened_item_is_closed(self,
                                                                                             update_closed_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_closed_work_items_setup
        # re-open closed item 1 day after close. Original lead time was 7 days
        work_items_list[0]['updated_at'] = work_items_list[0]['updated_at'] + timedelta(days=1)
        work_items_list[0]['state'] = 'open'
        api.update_work_items(organization_key, work_items_source_key, work_items_list)

        # put into wip 2 days after open
        work_items_list[0]['updated_at'] = work_items_list[0]['updated_at'] + timedelta(days=2)
        work_items_list[0]['state'] = 'wip'
        api.update_work_items(organization_key, work_items_source_key, work_items_list)

        # close it again 3 days after wip transition
        work_items_list[0]['updated_at'] = work_items_list[0]['updated_at'] + timedelta(days=3)
        work_items_list[0]['state'] = 'closed'
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list)
        assert result['success']
        with db.orm_session() as session:
            work_item = model.WorkItem.find_by_work_item_key(session, work_items_list[0]['key'])
            current_delivery_cycle = work_item.current_delivery_cycle
            assert len(current_delivery_cycle.delivery_cycle_durations) == 3
            assert {
                       duration.state: int(
                           duration.cumulative_time_in_state / (
                                   3600 * 24) if duration.cumulative_time_in_state is not None else 0
                       )
                       for duration in current_delivery_cycle.delivery_cycle_durations
                   } == {
                       'open': 2,
                       'wip': 3,
                       'closed': 0
                   }

    def it_does_not_update_delivery_cycle_when_transition_does_not_involve_closed_state(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_work_items_setup
        # check count before update
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycles \
            where lead_time is NULL and end_date is NULL").scalar() == 2
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list)
        assert result['success']
        assert db.connection().execute(
            "select count(*) from analytics.work_item_delivery_cycles").scalar() == 2
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycles \
            where lead_time is NULL and end_date is NULL").scalar() == 2

    def it_updates_cycle_time_and_spec_cycle_time_for_closed_work_items(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_work_items_setup
        work_items_list[0]['updated_at'] = datetime.utcnow() - timedelta(days=3)
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list[0:])
        assert result['success']
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles join analytics.work_items on work_item_delivery_cycles.work_item_id=work_items.id\
            where work_items.key='{work_items_list[0]['key']}' and cycle_time is NULL").scalar() == 1
        work_items_list[0]['state'] = 'wip'
        work_items_list[0]['updated_at'] = datetime.utcnow() - timedelta(days=2)
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list[0:])
        assert result['success']
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles join analytics.work_items on work_item_delivery_cycles.work_item_id=work_items.id\
                    where work_items.key='{work_items_list[0]['key']}' and cycle_time is NULL").scalar() == 1
        work_items_list[0]['state'] = 'complete'
        work_items_list[0]['updated_at'] = datetime.utcnow() - timedelta(days=1)
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list[0:])
        assert result['success']
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles join analytics.work_items on work_item_delivery_cycles.work_item_id=work_items.id\
                    where work_items.key='{work_items_list[0]['key']}' and cycle_time is NULL").scalar() == 1
        work_items_list[0]['state'] = 'closed'
        work_items_list[0]['updated_at'] = datetime.utcnow()
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list[0:])
        assert result['success']
        _delivery_cycle_id, cycle_time, spec_cycle_time = db.connection().execute(
            f"select delivery_cycle_id, cycle_time, spec_cycle_time from analytics.work_item_delivery_cycles join analytics.work_items on work_item_delivery_cycles.delivery_cycle_id=work_items.current_delivery_cycle_id\
                    where work_items.key='{work_items_list[0]['key']}' and cycle_time is not NULL").fetchall()[0]
        expected_cycle_time = db.connection().execute(
            f"select sum(cumulative_time_in_state) from analytics.work_item_delivery_cycle_durations\
                                             where state in ('open', 'wip', 'complete') and delivery_cycle_id={_delivery_cycle_id}").fetchall()[
            0][0]
        assert expected_cycle_time == cycle_time
        assert cycle_time == spec_cycle_time

    def it_updates_latency_to_zero_for_work_items_without_commits(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_work_items_setup
        closed_date = datetime.utcnow()

        work_items_list[0]['state'] = 'closed'
        work_items_list[0]['updated_at'] = closed_date

        result = api.update_work_items(organization_key, work_items_source_key, work_items_list[0:])
        assert result['success']
        assert db.connection().execute(
            f"select latency from analytics.work_item_delivery_cycles join analytics.work_items "
            f"on work_item_delivery_cycles.delivery_cycle_id = work_items.current_delivery_cycle_id "
            f"where work_items.key='{work_items_list[0]['key']}'"
        ).scalar() == 0

    def it_updates_latency_for_closed_work_items_with_commits(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_work_items_setup

        closed_date = datetime.utcnow()
        # Update commits stats for the current delivery cycle.
        with db.orm_session() as session:
            work_item = model.WorkItem.find_by_work_item_key(session, work_items_list[0]['key'])
            dc = work_item.current_delivery_cycle
            dc.commit_count = 2
            dc.latest_commit = closed_date - timedelta(days=3)
            dc.earliest_commit = dc.latest_commit - timedelta(days=2)

        work_items_list[0]['state'] = 'closed'
        work_items_list[0]['updated_at'] = closed_date

        result = api.update_work_items(organization_key, work_items_source_key, work_items_list[0:])
        assert result['success']
        assert db.connection().execute(
            f"select latency from analytics.work_item_delivery_cycles join analytics.work_items "
            f"on work_item_delivery_cycles.delivery_cycle_id = work_items.current_delivery_cycle_id "
            f"where work_items.key='{work_items_list[0]['key']}'"
        ).scalar() == 3 * 24 * 3600

    def it_updates_spec_cycle_time_for_closed_work_items_with_commits(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_work_items_setup

        # Setting closed date same as approximately the time of transition to open state
        closed_date = datetime.utcnow() - timedelta(days=5)
        # Update commits stats for the current delivery cycle.
        with db.orm_session() as session:
            work_item = model.WorkItem.find_by_work_item_key(session, work_items_list[0]['key'])
            # Setting work item closed date to old date just to ensure commits
            # are not newer than work item create date from source
            work_item.created_at = closed_date - timedelta(days=6)
            dc = work_item.current_delivery_cycle
            dc.commit_count = 2
            dc.latest_commit = closed_date - timedelta(days=3)
            dc.earliest_commit = dc.latest_commit - timedelta(days=2)

        work_items_list[0]['state'] = 'closed'
        work_items_list[0]['updated_at'] = closed_date

        result = api.update_work_items(organization_key, work_items_source_key, work_items_list[0:])
        assert result['success']
        assert db.connection().execute(
            f"select spec_cycle_time from analytics.work_item_delivery_cycles join analytics.work_items "
            f"on work_item_delivery_cycles.delivery_cycle_id = work_items.current_delivery_cycle_id "
            f"where work_items.key='{work_items_list[0]['key']}'"
        ).scalar() == 5 * 24 * 3600

    def it_set_spec_cycle_time_to_cycle_time_for_closed_work_items_with_commits_when_commit_cycle_time_is_smaller(self,
                                                                                                                  update_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_work_items_setup

        closed_date = datetime.utcnow()
        # Update commits stats for the current delivery cycle.
        with db.orm_session() as session:
            work_item = model.WorkItem.find_by_work_item_key(session, work_items_list[0]['key'])
            dc = work_item.current_delivery_cycle
            dc.commit_count = 2
            dc.latest_commit = closed_date - timedelta(days=1)
            dc.earliest_commit = dc.latest_commit - timedelta(days=1)

        work_items_list[0]['state'] = 'closed'
        work_items_list[0]['updated_at'] = closed_date

        result = api.update_work_items(organization_key, work_items_source_key, work_items_list[0:])
        assert result['success']
        cycle_time, spec_cycle_time = db.connection().execute(
            f"select cycle_time, spec_cycle_time from analytics.work_item_delivery_cycles join analytics.work_items "
            f"on work_item_delivery_cycles.delivery_cycle_id = work_items.current_delivery_cycle_id "
            f"where work_items.key='{work_items_list[0]['key']}'"
        ).fetchone()

        assert cycle_time == spec_cycle_time

    def it_updates_cycle_time_and_spec_cycle_time_only_for_current_delivery_cycle(self, update_closed_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_closed_work_items_setup
        result = api.import_new_work_items(organization_key, work_items_source_key, work_items_list[0:])
        assert result['success']
        # cycle time will be null as work item imported in closed state
        _delivery_cycle_1_id = db.connection().execute(
            "select delivery_cycle_id from analytics.work_item_delivery_cycles \
            where lead_time is not NULL and end_date is not NULL and cycle_time is NULL").scalar()
        # reopen the work item to create new delivery cycle
        work_items_list[0]['state'] = 'open'
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list[0:])
        # close again to update cycle time
        work_items_list[0]['state'] = 'closed'
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list[0:])
        assert result['success']
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycles \
            where delivery_cycle_id={_delivery_cycle_1_id} and cycle_time is NULL").scalar() == 1
        _delivery_cycle_2_id, cycle_time, spec_cycle_time = db.connection().execute(
            f"select delivery_cycle_id, cycle_time, spec_cycle_time from analytics.work_item_delivery_cycles join analytics.work_items on work_item_delivery_cycles.delivery_cycle_id=work_items.current_delivery_cycle_id\
                            where work_items.key='{work_items_list[0]['key']}' and cycle_time is not NULL").fetchall()[
            0]
        expected_cycle_time = db.connection().execute(
            f"select sum(cumulative_time_in_state) from analytics.work_item_delivery_cycle_durations\
                                                     where state in ('open', 'wip', 'complete') and delivery_cycle_id={_delivery_cycle_2_id}").fetchall()[
            0][0]
        assert _delivery_cycle_1_id != _delivery_cycle_2_id
        assert expected_cycle_time == cycle_time
        assert cycle_time == spec_cycle_time


class TestWorkItemDeliveryCycleDurations:

    def it_calculates_delivery_cycle_durations_for_new_work_items(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        work_items = []
        work_items.extend([
            dict(
                key=uuid.uuid4().hex,
                name=str(i),
                display_id=str(i),
                **work_items_common()
            )
            for i in range(0, 10)]
        )
        work_items[0]['state'] = 'closed'
        work_items[0]['created_at'] = datetime.utcnow() - timedelta(days=7)
        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        assert db.connection().execute(
            'select count(*) from analytics.work_item_delivery_cycle_durations').scalar() == 20
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_durations \
                where state='closed' and cumulative_time_in_state is NULL"
        ).scalar() == 1
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_durations \
                where cumulative_time_in_state>0 and state='created'"
        ).scalar() == 10

    def it_updates_delivery_cycle_durations_for_updated_work_items(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_work_items_setup
        work_items_list[0]['state'] = 'wip'
        work_items_list[0]['updated_at'] = datetime.utcnow()
        work_items_list[1]['state'] = 'closed'
        work_items_list[1]['updated_at'] = datetime.utcnow()
        result = api.update_work_items(organization_key, work_items_source_key, work_items_list)
        assert result['success']
        assert db.connection().execute(
            'select count(*) from analytics.work_item_delivery_cycle_durations').scalar() == 6
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_durations \
                where state='closed' and cumulative_time_in_state is NULL"
        ).scalar() == 1
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_durations \
                where state='wip' and cumulative_time_in_state is NULL"
        ).scalar() == 1
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_durations \
                where cumulative_time_in_state>0 and state='created'"
        ).scalar() == 2

    def it_updates_delivery_cycles_durations_for_work_item_transitioning_from_created_to_closed_through_different_states(
            self, work_items_setup):
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
        # import all work items first
        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        assert db.connection().execute(
            'select count(*) from analytics.work_item_delivery_cycle_durations').scalar() == 10

        # work_item 1 changes state to 'wip'
        work_items[0]['state'] = 'wip'
        # work_item 2 changes state to 'backlog'
        work_items[1]['state'] = 'backlog'
        # work_item 3 changes state to 'complete'
        work_items[2]['state'] = 'complete'
        result = api.update_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        assert db.connection().execute(
            "select count(*) from analytics.work_item_delivery_cycle_durations").scalar() == 13
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_durations where state='open' and cumulative_time_in_state is not NULL").scalar() == 3
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_durations \
                where state in ('wip', 'backlog', 'complete') and cumulative_time_in_state is NULL"
        ).scalar() == 3

        # work_item 1 changes state to 'complete'
        work_items[0]['state'] = 'complete'
        # work_item 2 changes state to 'ready for development'
        work_items[1]['state'] = 'ready for development'
        # work_item 3 changes state to 'closed'
        work_items[2]['state'] = 'closed'
        # update again
        result = api.update_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        assert db.connection().execute(
            "select count(*) from analytics.work_item_delivery_cycle_durations").scalar() == 16
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_durations \
                where state in ('wip', 'backlog', 'complete') and cumulative_time_in_state is not NULL").scalar() == 3
        assert db.connection().execute(
            "select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_durations \
                where state in ('complete', 'ready for development', 'closed') and cumulative_time_in_state is NULL"
        ).scalar() == 3

    def it_recomputes_cumulative_time_in_a_state_when_state_is_revisited(self, work_items_setup):
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
        work_items[0]['updated_at'] = work_items[0]['created_at'] + timedelta(days=1)
        # import all work items first
        result = api.import_new_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']

        # work_item 1 changes state to 'wip'
        work_items[0]['state'] = 'wip'
        work_items[0]['updated_at'] = work_items[0]['updated_at'] + timedelta(days=1)

        result = api.update_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        assert db.connection().execute(
            "select count(*) from analytics.work_item_delivery_cycle_durations").scalar() == 11

        # work_item 1 changes state to 'complete'
        work_items[0]['state'] = 'complete'
        work_items[0]['updated_at'] = work_items[0]['updated_at'] + timedelta(days=1)
        # update again
        result = api.update_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        assert db.connection().execute(
            "select count(*) from analytics.work_item_delivery_cycle_durations").scalar() == 12

        cumulative_time_in_wip = db.connection().execute(
            "select cumulative_time_in_state from analytics.work_item_delivery_cycle_durations where state='wip'").fetchall()[
            0][0]

        # work item 1 moves to 'wip' again, no change expected in cumulative time yet
        work_items[0]['state'] = 'wip'
        work_items[0]['updated_at'] = work_items[0]['updated_at'] + timedelta(days=1)
        result = api.update_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        # check there is still only one row with state 'wip'
        assert db.connection().execute(
            "select count(cumulative_time_in_state) from analytics.work_item_delivery_cycle_durations where state='wip'").scalar() == 1
        updated_cumulative_time_in_wip = db.connection().execute(
            "select cumulative_time_in_state from analytics.work_item_delivery_cycle_durations where state='wip'").fetchall()[
            0][0]
        assert updated_cumulative_time_in_wip == cumulative_time_in_wip

        # Changing state again so that 'wip' state has more time to be added
        work_items[0]['state'] = 'complete'
        work_items[0]['updated_at'] = work_items[0]['updated_at'] + timedelta(days=1)
        result = api.update_work_items(organization_key, work_items_source_key, work_items)
        assert result['success']
        # check there is still only one row with state 'wip'
        assert db.connection().execute(
            "select count(cumulative_time_in_state) from analytics.work_item_delivery_cycle_durations where state='wip'").scalar() == 1
        updated_cumulative_time_in_wip = db.connection().execute(
            "select cumulative_time_in_state from analytics.work_item_delivery_cycle_durations where state='wip'").fetchall()[
            0][0]
        assert updated_cumulative_time_in_wip > cumulative_time_in_wip


class TestMoveWorkItem:
    class TestWhenTargetExists:

        @pytest.fixture()
        def setup(self, update_work_items_setup):
            organization_key, work_items_source_key, work_items = update_work_items_setup
            # create another work items source
            with db.orm_session() as session:
                source_work_items_source = WorkItemsSource.find_by_work_items_source_key(
                    session, work_items_source_key=work_items_source_key
                )
                target_work_items_source = WorkItemsSource(
                    organization_id=source_work_items_source.organization_id,
                    organization_key=organization_key,
                    **dict_merge(work_item_source_common(), dict(key=uuid.uuid4().hex, source_id=str(uuid.uuid4())))
                )
                state_map_entries = [
                    dict(state='created', state_type=WorkItemsStateType.backlog.value),
                    dict(state='open', state_type=WorkItemsStateType.open.value),
                    dict(state='wip', state_type=WorkItemsStateType.wip.value),
                    dict(state='complete', state_type=WorkItemsStateType.complete.value),
                    dict(state='closed', state_type=WorkItemsStateType.closed.value),
                    dict(state='done', state_type=WorkItemsStateType.closed.value)
                ]
                target_work_items_source.init_state_map(state_map_entries)
                session.add(
                    target_work_items_source
                )
            yield Fixture(
                organization_key=organization_key,
                source_work_items_source=source_work_items_source,
                target_work_items_source=target_work_items_source,
                work_items=work_items
            )

        def it_updates_work_items_source_id_and_display_id(self, setup):
            fixture = setup
            work_item = fixture.work_items[0]
            work_item['display_id'] = 'MOVED-1'
            result = api.move_work_item(fixture.organization_key, fixture.source_work_items_source.key,
                                        fixture.target_work_items_source.key, work_item)
            assert result
            assert db.connection().execute(
                f"select count(id) from analytics.work_items where key='{work_item['key']}' and "
                f"work_items_source_id={fixture.source_work_items_source.id}").scalar() == 0

            assert db.connection().execute(f"select count(id) from analytics.work_items "
                                           f"where key='{work_item['key']}' and display_id='MOVED-1' "
                                           f"and work_items_source_id={fixture.target_work_items_source.id}").scalar() == 1

        def it_updates_work_items_source_id_display_id_and_state(self, setup):
            fixture = setup
            work_item = fixture.work_items[0]
            work_item['display_id'] = 'MOVED-1'
            work_item['state'] = 'created'
            result = api.move_work_item(fixture.organization_key, fixture.source_work_items_source.key,
                                        fixture.target_work_items_source.key, work_item)
            assert result
            assert db.connection().execute(
                f"select count(id) from analytics.work_items where key='{work_item['key']}' and "
                f"work_items_source_id={fixture.source_work_items_source.id}").scalar() == 0

            assert db.connection().execute(f"select count(id) from analytics.work_items "
                                           f"where key='{work_item['key']}' and display_id='MOVED-1' "
                                           f"and work_items_source_id={fixture.target_work_items_source.id} and state='created' "
                                           f"and state_type='{WorkItemsStateType.backlog.value}'").scalar() == 1

    class TestWhenTargetDoesNotExist:

        @pytest.fixture()
        def setup(self, update_work_items_setup):
            organization_key, work_items_source_key, work_items = update_work_items_setup
            with db.orm_session() as session:
                source_work_items_source = WorkItemsSource.find_by_work_items_source_key(
                    session, work_items_source_key=work_items_source_key
                )
            yield Fixture(
                organization_key=organization_key,
                source_work_items_source=source_work_items_source,
                work_items=work_items
            )

        def it_sets_is_moved_as_true(self, setup):
            fixture = setup
            work_item = fixture.work_items[0]
            work_item['is_moved'] = True
            work_item['is_moved_from_current_source'] = True
            result = api.move_work_item(fixture.organization_key, fixture.source_work_items_source.key,
                                        None, work_item)
            assert result
            assert db.connection().execute(
                f"select count(id) from analytics.work_items where key='{work_item['key']}' and "
                f"work_items_source_id={fixture.source_work_items_source.id} and is_moved_from_current_source=TRUE ").scalar() == 1


class TestDeleteWorkItem:

    @pytest.fixture()
    def setup(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        yield Fixture(
            organization_key=organization_key,
            work_items_source_key=work_items_source_key,
            work_items=work_items
        )

    def it_deletes_work_item_when_there_is_an_open_delivery_cycle_and_no_closed_cycles(self, setup):
        fixture = setup
        work_item = fixture.work_items[0]
        work_item['deleted_at'] = datetime.utcnow()
        result = api.delete_work_item(fixture.organization_key, fixture.work_items_source_key, work_item)
        assert result
        assert db.connection().execute(
            f"select count(id) from analytics.work_items where key='{work_item['key']}'").scalar() == 0

    def it_updates_deleted_at_time_for_a_work_item_with_closed_delivery_cycle(self, setup):
        fixture = setup
        work_item = fixture.work_items[0]
        work_item['state'] = 'closed'
        api.update_work_items(fixture.organization_key, fixture.work_items_source_key, [work_item])
        work_item['deleted_at'] = datetime.utcnow()
        result = api.delete_work_item(fixture.organization_key, fixture.work_items_source_key, work_item)
        assert result
        assert db.connection().execute(
            f"select count(id) from analytics.work_items where key='{work_item['key']}' and deleted_at='{work_item['deleted_at']}'").scalar() == 1

    def it_updates_deleted_at_time_for_a_work_item_whic_was_reopened(self, setup):
        fixture = setup
        work_item = fixture.work_items[0]
        work_item['state'] = 'closed'
        api.update_work_items(fixture.organization_key, fixture.work_items_source_key, [work_item])
        work_item['state'] = 'open'
        api.update_work_items(fixture.organization_key, fixture.work_items_source_key, [work_item])
        work_item['deleted_at'] = datetime.utcnow()
        result = api.delete_work_item(fixture.organization_key, fixture.work_items_source_key, work_item)
        assert result
        assert db.connection().execute(
            f"select count(id) from analytics.work_items where key='{work_item['key']}' and deleted_at='{work_item['deleted_at']}'").scalar() == 1


class TestWorkTracking(WorkItemsTest):
    class TestSetCustomTypeMappingOnWorkItemImportAndUpdate:

        @pytest.fixture()
        def setup(self, setup):
            fixture = setup

            # update the work_items_source with a custom type map
            with db.orm_session() as session:
                work_items_source = WorkItemsSource.find_by_work_items_source_key(session,
                                                                                  fixture.work_items_source_key)
                work_items_source.update_custom_type_mappings([
                    dict(
                        labels=['custom_type:Feature'],
                        work_item_type=WorkItemType.epic.value
                    ),
                    dict(
                        labels=['tech-debt', 'performance-optimization'],
                        work_item_type=WorkItemType.task.value
                    ),
                    dict(
                        labels=['defect', 'CVE'],
                        work_item_type=WorkItemType.bug.value
                    )
                ])
                session.flush()

            yield fixture

        def it_updates_the_work_item_types_using_the_custom_type_mapping_on_import(self, setup):
            fixture = setup

            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            work_items_common = fixture.work_items_common

            test_work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name='Test Work Items',
                    display_id='PP-431',
                    **dict_merge(
                        work_items_common,
                        dict(
                            work_item_type=WorkItemType.story.value,
                            is_bug=False,
                            is_epic=False,
                            tags=['custom_type:Feature']
                        )
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Test Work Items2',
                    display_id='PP-431-1',
                    **dict_merge(
                        work_items_common,
                        dict(
                            work_item_type=WorkItemType.story.value,
                            is_bug=False,
                            is_epic=False,
                            tags=['tech-debt']
                        )
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Test Work Items3',
                    display_id='PP-431-2',
                    **dict_merge(
                        work_items_common,
                        dict(
                            work_item_type=WorkItemType.story.value,
                            is_bug=False,
                            is_epic=False,
                            tags=['random-tag']
                        )
                    )
                )

            ]

            result = api.import_new_work_items(organization_key, work_items_source_key, test_work_items)
            assert result['success']
            with db.orm_session() as session:
                # this tests the single label case in the mapping
                work_item = model.WorkItem.find_by_work_item_key(session, test_work_items[0]['key'])
                assert work_item.work_item_type == WorkItemType.epic.value

                # this tests the multiple label case in the mapping
                work_item = model.WorkItem.find_by_work_item_key(session, test_work_items[1]['key'])
                assert work_item.work_item_type == WorkItemType.task.value

                # this tests the case where the label does not match in the mapping
                work_item = model.WorkItem.find_by_work_item_key(session, test_work_items[2]['key'])
                assert work_item.work_item_type == WorkItemType.story.value

        def it_sets_the_is_epic_flag_when_the_custom_type_maps_to_an_epic(self, setup):
            fixture = setup

            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            work_items_common = fixture.work_items_common

            test_work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name='Test Work Items',
                    display_id='PP-431',
                    **dict_merge(
                        work_items_common,
                        dict(
                            work_item_type=WorkItemType.story.value,
                            is_bug=False,
                            is_epic=False,
                            tags=['custom_type:Feature']
                        )
                    )
                )
            ]

            result = api.import_new_work_items(organization_key, work_items_source_key, test_work_items)
            assert result['success']
            with db.orm_session() as session:
                # this tests the single label case in the mapping
                work_item = model.WorkItem.find_by_work_item_key(session, test_work_items[0]['key'])
                assert work_item.is_epic

        def it_sets_the_is_bug_flag_when_the_custom_type_maps_to_a_bug(self, setup):
            fixture = setup

            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            work_items_common = fixture.work_items_common

            test_work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name='Test Work Items',
                    display_id='PP-431',
                    **dict_merge(
                        work_items_common,
                        dict(
                            work_item_type=WorkItemType.story.value,
                            is_bug=False,
                            is_epic=False,
                            tags=['CVE']
                        )
                    )
                )
            ]

            result = api.import_new_work_items(organization_key, work_items_source_key, test_work_items)
            assert result['success']
            with db.orm_session() as session:
                # this tests the single label case in the mapping
                work_item = model.WorkItem.find_by_work_item_key(session, test_work_items[0]['key'])
                assert work_item.is_bug

        def it_chooses_the_first_matching_label_when_there_are_multiple_matches(self, setup):
            fixture = setup

            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            work_items_common = fixture.work_items_common

            test_work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name='Test Work Items',
                    display_id='PP-431',
                    **dict_merge(
                        work_items_common,
                        dict(
                            work_item_type=WorkItemType.story.value,
                            is_bug=False,
                            is_epic=False,
                            tags=['custom_type:Feature', 'CVE']
                        )
                    )
                )
            ]

            result = api.import_new_work_items(organization_key, work_items_source_key, test_work_items)
            assert result['success']
            with db.orm_session() as session:
                # this tests the single label case in the mapping
                work_item = model.WorkItem.find_by_work_item_key(session, test_work_items[0]['key'])
                assert work_item.work_item_type == WorkItemType.epic.value

        def it_sets_the_custom_map_for_work_items_on_updates(self, setup):
            fixture = setup

            organization_key = fixture.organization_key
            work_items_source_key = fixture.work_items_source_key
            work_items_common = fixture.work_items_common

            test_work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name='Test Work Items',
                    display_id='PP-431',
                    **dict_merge(
                        work_items_common,
                        dict(
                            work_item_type=WorkItemType.story.value,
                            is_bug=False,
                            is_epic=False,
                            tags=['custom_type:Feature']
                        )
                    )
                )
            ]
            # first import the work item
            result = api.import_new_work_items(organization_key, work_items_source_key, test_work_items)
            # now change the custom type and see if it gets remapped on update
            test_work_items[0]['tags'] = ['CVE']
            result = api.update_work_items(organization_key, work_items_source_key, test_work_items)

            assert result['success']
            with db.orm_session() as session:
                # this tests the single label case in the mapping
                work_item = model.WorkItem.find_by_work_item_key(session, test_work_items[0]['key'])
                assert work_item.work_item_type == WorkItemType.bug.value
