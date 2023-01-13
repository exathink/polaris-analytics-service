# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
import logging

import polaris.analytics.db.commands
from polaris.common import db
from polaris.analytics.db import api as db_api
from polaris.analytics import api
from polaris.analytics import publish

from ..interfaces import WorkItemsStateType, WorkItemsStateFlowType, WorkItemsStateReleaseStatusType
from ..input_types import FlowMetricsSettingsInput, AnalysisPeriodsInput, WipInspectorSettingsInput
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
    flow_type = WorkItemsStateFlowType(required=False)
    release_status = WorkItemsStateReleaseStatusType(required=False)


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
        with db.orm_session() as session:
            update_state_maps_result = db_api.update_project_work_items_source_state_mappings(update_project_state_maps_input, join_this=session)
            if update_state_maps_result['success']:
                for updated_work_items_source in update_state_maps_result['work_items_sources']:
                        publish.recalculate_cycle_times_for_work_items_source(
                            update_project_state_maps_input.project_key,
                            updated_work_items_source['source_key'],
                            rebuild_delivery_cycles=updated_work_items_source['should_rebuild_delivery_cycles']
                        )

                return UpdateProjectStateMaps(success=True)
            else:
                return UpdateProjectStateMaps(success=False, error_message=update_state_maps_result.get('exception'))


class WorkItemsInfo(graphene.InputObjectType):
    work_item_key = graphene.String(required=True)
    budget = graphene.Float(required=False)
    # can add more fields as per need


class UpdateProjectWorkItemsInput(graphene.InputObjectType):
    project_key = graphene.String(required=True)
    work_items_info = graphene.List(WorkItemsInfo, required=True)


class UpdateProjectWorkItemsStatus(graphene.ObjectType):
    work_items_keys = graphene.List(graphene.String, required=False)
    success = graphene.Boolean(required=True)
    message = graphene.String(required=False)
    exception = graphene.String(required=False)


class UpdateProjectWorkItems(graphene.Mutation):
    class Arguments:
        update_project_work_items_input = UpdateProjectWorkItemsInput(required=True)

    update_status = graphene.Field(UpdateProjectWorkItemsStatus)

    def mutate(self, info, update_project_work_items_input):
        logger.info('UpdateProjectWorkItems called')
        result = db_api.update_project_work_items(project_work_items=update_project_work_items_input)
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
    wip_inspector_settings = graphene.Field(WipInspectorSettingsInput, required=False)


class UpdateProjectSettings(graphene.Mutation):
    class Arguments:
        update_project_settings_input = UpdateProjectSettingsInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()

    def mutate(self, info, update_project_settings_input):
        logger.info('UpdateProject Settings called')
        result = db_api.update_project_settings(update_project_settings_input)
        return UpdateProjectSettings(success=result['success'], error_message=result.get('exception'))


class RepositoryExclusionState(graphene.InputObjectType):
    repository_key = graphene.String(required=True)
    excluded = graphene.Boolean(required=True)


class UpdateProjectExcludedRepositoriesInput(graphene.InputObjectType):
    project_key = graphene.String(required=True)
    exclusions = graphene.List(RepositoryExclusionState, required=True)


class UpdateProjectExcludedRepositories(graphene.Mutation):
    class Arguments:
        update_project_excluded_repositories_input = UpdateProjectExcludedRepositoriesInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()

    def mutate(self, info, update_project_excluded_repositories_input):
        logger.info('UpdateProject Excluded Repositories called')
        result = db_api.update_project_excluded_repositories(update_project_excluded_repositories_input)
        return UpdateProjectExcludedRepositories(success=result['success'], error_message=result.get('exception'))


class ProjectMutationsMixin:
    archive_project = ArchiveProject.Field()
    update_project_state_maps = UpdateProjectStateMaps.Field()
    update_project_settings = UpdateProjectSettings.Field()
    update_project_work_items = UpdateProjectWorkItems.Field()
    update_project_excluded_repositories = UpdateProjectExcludedRepositories.Field()
