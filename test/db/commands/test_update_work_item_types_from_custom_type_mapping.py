# -*- coding: utf-8 -*-

#  Copyright (c) Exathink, LLC  2016-2023.
#  All rights reserved
#

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import pytest
import uuid


from polaris.utils.collections import dict_merge, Fixture
from polaris.analytics.db.enums import WorkItemType
from polaris.analytics.db import api, commands

from test.fixtures.project_work_items import *

class TestCustomMappingWorkItemTypeUpdate(ProjectWorkItemsTest):

    class TestWorkItemUpdate:

        @pytest.fixture
        def setup(self, setup):
            fixture = setup
            organization = fixture.organization
            work_items_source = fixture.work_items_source
            work_items_common = fixture.work_items_common

            test_work_items = [
                dict(
                    key=uuid.uuid4(),
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
                    key=uuid.uuid4(),
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
                    key=uuid.uuid4(),
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
            result = api.import_new_work_items(organization.key, work_items_source.key, test_work_items)
            assert result['success']

            yield Fixture(
                parent=fixture,
                work_items=test_work_items
            )

        def it_updates_work_item_types_for_existing_work_items_with_the_custom_type_mapping(self, setup):
            fixture = setup
            project = fixture.project
            project_key = str(project.key)
            work_items_source_keys = [
                str(project.work_items_sources[0].key)
            ]
            test_work_item = fixture.work_items[0]

            api.update_project_custom_type_mappings(project_key, work_items_source_keys, custom_type_mappings = [
                dict(
                    labels=['custom_type:Feature'],
                    work_item_type=WorkItemType.epic.value
                )
            ])

            result = commands.update_work_item_types_from_custom_type_mapping(project_key, work_items_source_keys)
            assert result['success']
            assert result['updated'] == 1
            assert db.connection().execute(f"select work_item_type from analytics.work_items where key='{test_work_item['key']}'").scalar() == 'epic'

        def it_updates_mappings_with_multiple_labels_mapped_to_a_single_type(self, setup):
            fixture = setup
            project = fixture.project
            project_key = str(project.key)
            work_items_source_keys = [
                str(project.work_items_sources[0].key)
            ]
            work_items = fixture.work_items

            test_keys = {str(work_items[0]['key']), str(work_items[2]['key'])}

            api.update_project_custom_type_mappings(project_key, work_items_source_keys, custom_type_mappings = [
                dict(
                    labels=['custom_type:Feature', 'random-tag'],
                    work_item_type=WorkItemType.epic.value
                )
            ])

            result = commands.update_work_item_types_from_custom_type_mapping(project_key, work_items_source_keys)
            assert result['success']
            assert result['updated'] == 2
            rows = db.connection().execute(f"select key from analytics.work_items where work_item_type='epic'").fetchall()
            assert {
                str(row.key)
                for row in rows
            } == test_keys


        def it_updates_multiple_types(self, setup):
            fixture = setup
            project = fixture.project
            project_key = str(project.key)
            work_items_source_keys = [
                str(project.work_items_sources[0].key)
            ]
            work_items = fixture.work_items


            api.update_project_custom_type_mappings(project_key, work_items_source_keys, custom_type_mappings = [
                dict(
                    labels=['custom_type:Feature'],
                    work_item_type=WorkItemType.epic.value
                ),
                dict(
                    labels=['random-tag'],
                    work_item_type=WorkItemType.bug.value
                ),
                dict(
                    labels=['tech-debt'],
                    work_item_type=WorkItemType.task.value
                ),

            ])

            result = commands.update_work_item_types_from_custom_type_mapping(project_key, work_items_source_keys)
            assert result['success']
            assert result['updated'] == 3
            rows = db.connection().execute(f"select key, work_item_type from analytics.work_items").fetchall()
            assert {
                (str(row.key), row.work_item_type)
                for row in rows
            } == {
                (str(work_items[0]['key']), 'epic'),
                (str(work_items[1]['key']), 'task'),
                (str(work_items[2]['key']), 'bug')
            }

        def it_updates_is_epic_flag_for_items_mapped_to_epics(self, setup):
            fixture = setup
            project = fixture.project
            project_key = str(project.key)
            work_items_source_keys = [
                str(project.work_items_sources[0].key)
            ]
            test_work_item = fixture.work_items[0]

            api.update_project_custom_type_mappings(project_key, work_items_source_keys, custom_type_mappings = [
                dict(
                    labels=['custom_type:Feature'],
                    work_item_type=WorkItemType.epic.value
                )
            ])

            result = commands.update_work_item_types_from_custom_type_mapping(project_key, work_items_source_keys)
            assert result['success']
            assert result['updated'] == 1
            assert db.connection().execute(f"select is_epic from analytics.work_items where key='{test_work_item['key']}'").scalar()
            assert db.connection().execute(
                f"select count(id) from analytics.work_items where key='{test_work_item['key']}' and is_epic").scalar() == 1

        def it_updates_is_bug_flag_for_items_mapped_to_bugs(self, setup):
            fixture = setup
            project = fixture.project
            project_key = str(project.key)
            work_items_source_keys = [
                str(project.work_items_sources[0].key)
            ]
            test_work_item = fixture.work_items[2]

            api.update_project_custom_type_mappings(project_key, work_items_source_keys, custom_type_mappings = [
                dict(
                    labels=['random-tag'],
                    work_item_type=WorkItemType.bug.value
                )
            ])

            result = commands.update_work_item_types_from_custom_type_mapping(project_key, work_items_source_keys)
            assert result['success']
            assert result['updated'] == 1
            assert db.connection().execute(f"select is_bug from analytics.work_items where key='{test_work_item['key']}'").scalar()
            assert db.connection().execute(
                f"select count(id) from analytics.work_items where key='{test_work_item['key']}' and is_bug").scalar() == 1