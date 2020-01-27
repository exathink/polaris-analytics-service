# -*- coding: utf-8 -

import logging

import graphene

from polaris.analytics.db.enums import WorkItemsSourceStateType
from polaris.analytics.db.model import WorkItemsSourceStateMap

logger = logging.getLogger('polaris.work_items_source_stat_map.mutations')

class StateMapParams(graphene.InputObjectType):
    state = graphene.String(required=True)
    stateType = graphene.String(required=True)

class WorkItemsSourceStateMap(graphene.InputObjectType):
    workItemsSourceKey = graphene.String(required=True)
    stateMaps = graphene.List(StateMapParams)

class UpdateProjectStateMapsInput(graphene.InputObjectType):
    projectKey = graphene.String(required=True)
    workItemsSourceStateMaps = graphene.List(WorkItemsSourceStateMap)

class UpdateProjectStateMaps(graphene.Mutation):
    class Arguments:
        update_project_state_maps_input = UpdateProjectStateMapsInput(required=True)

    success = graphene.Boolean()

    def mutate(self, info, update_project_state_maps_input):
        return UpdateProjectStateMaps(
            success=True
        )

class UpdateProjectStateMapsMixin:
    update_project_state_maps = UpdateProjectStateMaps.Field()