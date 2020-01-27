# -*- coding: utf-8 -

import logging

import graphene

logger = logging.getLogger('polaris.work_items_source_stat_map.mutations')

class StateMapParams(graphene.InputObjectType):
    state = graphene.String(required=True)
    state_type = graphene.String(required=True)

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

    def mutate(self, info, update_project_state_maps_input):
        return UpdateProjectStateMaps(
            success=True
        )

class UpdateProjectStateMapsMixin:
    update_project_state_maps = UpdateProjectStateMaps.Field()