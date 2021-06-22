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

from polaris.analytics.db.model import Team


@pytest.yield_fixture()
def setup_teams(org_repo_fixture, cleanup_teams):
    organization, projects, repositories = org_repo_fixture

    with db.orm_session() as session:
        team_a = dict(
            key=uuid.uuid4(),
            name='Team Alpha'
        )
        team_b = dict(
            key=uuid.uuid4(),
            name='Team Beta'
        )
        teams = [team_a, team_b]

        session.add(organization)
        for team in teams:
            organization.teams.append(Team(**team))

    yield Fixture(
        organization=organization,
        team_a=team_a,
        team_b=team_b,
        teams=teams
    )


@pytest.yield_fixture()
def cleanup_teams():
    yield

    db.connection().execute("delete from analytics.contributors_teams")
    db.connection().execute("delete from analytics.contributors")
    db.connection().execute("delete from analytics.teams")

