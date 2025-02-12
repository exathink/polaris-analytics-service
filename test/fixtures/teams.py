# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2021) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest
import uuid
from test.fixtures.graphql import org_repo_fixture
from polaris.utils.collections import Fixture
from polaris.common import db

from polaris.analytics.db.model import Team, Contributor, WorkItem


@pytest.fixture()
def setup_teams(org_repo_fixture, cleanup_teams):
    organization, projects, repositories = org_repo_fixture

    with db.orm_session() as session:
        session.expire_on_commit = False
        team_a = dict(
            key=uuid.uuid4(),
            name='Team Alpha'
        )
        team_b = dict(
            key=uuid.uuid4(),
            name='Team Beta'
        )
        team_c = dict(
            key=uuid.uuid4(),
            name='Team Delta'
        )
        teams = [team_a, team_b, team_c]

        session.add(organization)
        for team in teams:
            organization.teams.append(Team(**team))

    yield Fixture(
        organization=organization,
        team_a=team_a,
        team_b=team_b,
        team_c=team_c,
        teams=teams
    )


@pytest.fixture
def setup_team_assignments(setup_teams):
    fixture = setup_teams

    with db.orm_session() as session:
        joe = Contributor(
            name='Joe',
            key=uuid.uuid4()
        )
        alice = Contributor(
            name="Alice",
            key=uuid.uuid4()
        )
        arjun = Contributor(
            name='Arjun',
            key=uuid.uuid4()
        )
        team_a = fixture.team_a['key']
        team_b = fixture.team_b['key']

        session.add(joe)
        joe.assign_to_team(session, team_a)

        session.add(alice)
        alice.assign_to_team(session, team_b)

        session.add(arjun)
        arjun.assign_to_team(session, team_b)


    yield Fixture(
        parent=fixture,
        joe=joe,
        alice=alice,
        arjun=arjun
    )

@pytest.fixture()
def cleanup_teams():
    yield


# utilities
def map_work_items_to_team(team, work_items):
    with db.orm_session() as session:
        team = Team.find_by_key(session, team['key'])
        for wi in work_items:
            work_item = WorkItem.find_by_work_item_key(session, wi['key'])
            team.work_items.append(work_item)