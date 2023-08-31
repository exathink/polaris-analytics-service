# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
import graphene

from polaris.analytics.db import api as db_api
from ..team import Team
from polaris.utils.exceptions import ProcessingException

from ..input_types import FlowMetricsSettingsInput, AnalysisPeriodsInput, WipInspectorSettingsInput

logger = logging.getLogger('polaris.analytics.graphql')


class CreateTeamInput(graphene.InputObjectType):
    organization_key = graphene.String(required=True)
    name = graphene.String(required=True)


class CreateTeam(graphene.Mutation):
    class Arguments:
        create_team_input = CreateTeamInput(required=True)

    team = Team.Field()
    success = graphene.Boolean()
    errorMessage = graphene.String()

    def mutate(self, info, create_team_input):

        result = db_api.create_team(create_team_input.organization_key, create_team_input.name)

        if result['success']:
            return CreateTeam(
                team=Team.resolve_field(self, info, team_key=result['key']),
                success=True
            )
        else:
            return CreateTeam(
                team=None,
                success=False,
                errorMessage=f"{result['message']}: {result['exception']}"
            )


class UpdateTeamSettingsInput(graphene.InputObjectType):
    key = graphene.String(required=True)
    name = graphene.String(required=False)
    work_item_selectors = graphene.List(graphene.String, required=False)
    flow_metrics_settings = graphene.Field(FlowMetricsSettingsInput, required=False)
    analysis_periods = graphene.Field(AnalysisPeriodsInput, required=False)
    wip_inspector_settings = graphene.Field(WipInspectorSettingsInput, required=False)


class UpdateTeamSettings(graphene.Mutation):
    class Arguments:
        update_team_settings_input = UpdateTeamSettingsInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()

    def mutate(self, info, update_team_settings_input):
        logger.info('UpdateTeam Settings called')
        result = db_api.update_team_settings(update_team_settings_input)
        return UpdateTeamSettings(success=result['success'], error_message=result.get('exception'))


class TeamMutationsMixin:
    create_team = CreateTeam.Field()
    update_team_settings = UpdateTeamSettings.Field()
