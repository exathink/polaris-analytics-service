# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import pytest
from test.fixtures.contributors import *
from test.fixtures.teams import *
from polaris.utils.collections import Fixture

from polaris.messaging.test_utils import fake_send, mock_publisher, mock_channel
from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber

from polaris.analytics.messaging.messages import ContributorTeamAssignmentsChanged


class TestContributorTeamAssignmentsChanged:

    @pytest.fixture()
    def setup(self, setup_commits_for_contributor_updates, setup_teams):
        yield Fixture(
            organization_key=rails_organization_key,
            team_a=setup_teams.team_a,
            team_b=setup_teams.team_b,
            joe=joe_contributor_key,
            bill=billy_contributor_key
        )

    def it_publishes_responses_correctly(self, setup):
        fixture = setup

        message = fake_send(
            ContributorTeamAssignmentsChanged(
                send=dict(
                    organization_key=fixture.organization_key,
                    contributor_team_assignments=[
                        dict(
                            contributor_key=joe_contributor_key,
                            new_team_key=fixture.team_a['key'],
                            initial_assignment=True
                        )
                    ]
                )
            )
        )
        publisher = mock_publisher()
        channel = mock_channel()

        result = AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        assert result['success']
