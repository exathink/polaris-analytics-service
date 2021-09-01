# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
from polaris.analytics.db.model import Team, Organization
from polaris.utils.exceptions import ProcessingException


def create_team(session, organization_key, name):

    organization = Organization.find_by_organization_key(session, organization_key)
    if organization is not None:
        team_key = uuid.uuid4()
        organization.teams.append(
            Team(
                key=team_key,
                name=name
            )
        )
        return dict(
            key=team_key
        )

    else:
        raise ProcessingException(f"Organization with key {organization_key} was not found")


def update_team_settings(session, update_team_settings_input):
    team = Team.find_by_key(session, update_team_settings_input.key)
    if team is not None:
        team.update_settings(update_team_settings_input)
        return dict(
            key=team.key
        )
    else:
        raise ProcessingException(f'Could not find project with key: {update_team_settings_input.key}')