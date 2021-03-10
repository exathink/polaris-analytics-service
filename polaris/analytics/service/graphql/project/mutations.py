# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
import logging
from polaris.common import db
from polaris.analytics.db import api as db_api
from polaris.analytics import api
from ..interfaces import WorkItemsStateType
from ..input_types import FlowMetricsSettingsInput, AnalysisPeriodsInput
from polaris.utils.exceptions import ProcessingException

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
                project_name=api.archive_project(archive_project_input.project_key, join_this=session)
            )


# Update Project State Maps

class StateMapParams(graphene.InputObjectType):
    state = graphene.String(required=True)
    state_type = WorkItemsStateType(required=True)


class WorkItemsSourceStateMap(graphene.InputObjectType):
    work_items_source_key = graphene.String(required=True)

    state_maps = graphene.List(StateMapParams)


class UpdateProjectStateMapsInput(graphene.InputObjectType):
    project_key = graphene.String(required=True)
    work_items_source_state_maps = graphene.List(WorkItemsSourceStateMap)


class UpdateProjectStateMaps(graphene.Mutation):
    class Arguments:
        update_project_state_maps_input = UpdateProjectStateMapsInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()

    def mutate(self, info, update_project_state_maps_input):
        logger.info('UpdateProjectStateMaps called')
        result = db_api.update_project_work_items_source_state_mappings(update_project_state_maps_input)
        return UpdateProjectStateMaps(success=result['success'], error_message=result.get('exception'))


class WorkItemUpdatedInfo(graphene.InputObjectType):
    budget = graphene.Float(required=False)
    # can add more fields as per need


class WorkItemsInfo(graphene.InputObjectType):
    work_item_key = graphene.String(required=True)
    updated_info = graphene.Field(WorkItemUpdatedInfo, required=True)


class UpdateProjectWorkItemsStatus(graphene.ObjectType):
    work_items_keys = graphene.List(graphene.String, required=True)
    success = graphene.Boolean(required=True)
    message = graphene.String(required=False)
    exception = graphene.String(required=False)


class UpdateProjectWorkItems(graphene.Mutation):
    class Arguments:
        work_items_info = WorkItemsInfo(required=True)

    update_status = graphene.Field(UpdateProjectWorkItemsStatus)

    def mutate(self, info, work_items_info):
        logger.info('UpdateProjectWorkItems called')
        result = dict(
            work_items_keys=[],
            success=True,
            message='',
            exception=None
        )
        if result:
            return UpdateProjectWorkItems(
                UpdateProjectWorkItemsStatus(
                    work_items_keys=result.get('work_items_keys'),
                    success=result.get('success'),
                    message=result.get('message'),
                    exception=result.get('exception')
                )
            )
        else:
            raise ProcessingException('Could not update project work items')


# Update project settings


class UpdateProjectSettingsInput(graphene.InputObjectType):
    key = graphene.String(required=True)
    flow_metrics_settings = graphene.Field(FlowMetricsSettingsInput, required=False)
    analysis_periods = graphene.Field(AnalysisPeriodsInput, required=False)



class UpdateProjectSettings(graphene.Mutation):
    class Arguments:
        update_project_settings_input = UpdateProjectSettingsInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()

    def mutate(self, info, update_project_settings_input):
        logger.info('UpdateProject Settings called')
        result = db_api.update_project_settings(update_project_settings_input)
        return UpdateProjectSettings(success=result['success'], error_message=result.get('exception'))


class ProjectMutationsMixin:
    archive_project = ArchiveProject.Field()
    update_project_state_maps = UpdateProjectStateMaps.Field()
    update_project_settings = UpdateProjectSettings.Field()
    update_project_work_items = UpdateProjectWorkItems.Field()
