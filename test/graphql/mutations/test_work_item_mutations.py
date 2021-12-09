# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid

from unittest import mock
from unittest.mock import patch

from test.fixtures.work_items import *

from polaris.utils.collections import Fixture

from graphene.test import Client
from polaris.analytics.service.graphql import schema

from polaris.messaging.topics import AnalyticsTopic
from polaris.analytics.messaging.commands import ResolveCommitsForWorkItems

from polaris.messaging.test_utils import assert_topic_and_message

from polaris.analytics.service.graphql.work_item import mutations

class TestResolveCommitsForWorkItems:

    @pytest.fixture()
    def setup(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items_list = update_work_items_setup

        mutation = """
            mutation resolveCommitsForWorkItems($organizationKey: String!, $workItemsSourceKey: String!, $workItemKeys: [String]!) {
                resolveCommitsForWorkItems(resolveCommitsForWorkItemsInput: {
                    organizationKey: $organizationKey,
                    workItemsSourceKey: $workItemsSourceKey,
                    workItemKeys: $workItemKeys
                }) {
                    
                    success
        
                }
            }
        """

        yield Fixture(
            organization_key=organization_key,
            work_items_source_key=work_items_source_key,
            work_items_list=work_items_list,
            mutation=mutation
        )

    def it_publishes_the_resolve_commits_for_work_items_message(self, setup):
        fixture = setup

        client = Client(schema)

        with patch("polaris.analytics.publish.publish") as publish:
            result = client.execute(
                fixture.mutation,
                variable_values=dict(
                    organizationKey=fixture.organization_key,
                    workItemsSourceKey=fixture.work_items_source_key,
                    workItemKeys=[work_item['key'] for work_item in fixture.work_items_list]
                )
            )
        assert 'errors' not in result
        assert result['data']['resolveCommitsForWorkItems']['success']

        assert_topic_and_message(publish, AnalyticsTopic, ResolveCommitsForWorkItems)
