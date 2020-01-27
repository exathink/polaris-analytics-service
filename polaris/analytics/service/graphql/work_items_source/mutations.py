# -*- coding: utf-8 -

import logging

import graphene

from polaris.analytics.db.enums import WorkItemsSourceStateType
from polaris.analytics.db.model import WorkItemsSourceStateMap

logger = logging.getLogger('polaris.work_items_source_stat_map.mutations')

class UpdateProjectStateMapsInput(graphene.InputObjectType):
    projectKey = graphene.String(required=True)
    workItemsSourceStateMaps = WorkItemsSourceStateMap

class UpdateProjectStateMaps(graphene.Mutation):
    class Arguments:
        update_project_state_maps_input = UpdateProjectStateMapsInput(required=True)

    success = graphene.Boolean()

    def mutate(self, info, create_work_items_source_state_map_input):
        return UpdateProjectStateMaps(
            success=True
        )

class UpdateProjectStateMapsMixin:
    update_project_state_maps = UpdateProjectStateMaps.Field()