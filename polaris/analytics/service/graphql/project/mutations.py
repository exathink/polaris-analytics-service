# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
import logging
from polaris.common import db
from polaris.analytics.api import project
from polaris.analytics import api

logger = logging.getLogger('polaris.analytics.mutations')


class ArchiveProjectInput(graphene.InputObjectType):
    project_key = graphene.String(required=True)


class ArchiveProject(graphene.Mutation):
    class Arguments:
        archive_project_input = ArchiveProjectInput(required=True)

    project_name = graphene.String(required=True)

    def mutate(self, info, archive_project_input):
        with db.orm_session() as session:
            return ArchiveProject(
                project_name=project.archive_project(archive_project_input.project_key, join_this=session)
            )


# Update Project State Maps

class StateMapParams(graphene.InputObjectType):
    state = graphene.String(required=True)
    state_type = graphene.String(required=True)


class WorkItemsSourceStateMap(graphene.InputObjectType):
    work_items_source_key = graphene.String(required=True)
    #TODO Find out how to check for unique keys in stateMapParams here
    state_maps = graphene.List(StateMapParams)


class UpdateProjectStateMapsInput(graphene.InputObjectType):
    project_key = graphene.String(required=True)
    work_items_source_state_maps = graphene.List(WorkItemsSourceStateMap)


class UpdateProjectStateMaps(graphene.Mutation):
    class Arguments:
        update_project_state_maps_input = UpdateProjectStateMapsInput(required=True)

    success = graphene.Boolean()

    def mutate(self, info, update_project_state_maps_input):
        logger.info('UpdateProjectStateMaps called')
        with db.orm_session() as session:
            return UpdateProjectStateMaps(
                success=project.update_project_state_maps(update_project_state_maps_input, join_this=session)
         )


class ProjectMutationsMixin:
    archive_project = ArchiveProject.Field()
    update_project_state_maps = UpdateProjectStateMaps.Field()

