# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene


from polaris.graphql.selectable import Selectable
from polaris.graphql.selectable import CountableConnection
from polaris.analytics.service.graphql.interfaces import WorkItemInfo, WorkItemsSourceRef, WorkItemStateTransitions
from polaris.graphql.selectable import ConnectionResolverMixin
from polaris.analytics.service.graphql.interface_mixins import KeyIdResolverMixin, WorkItemInfoResolverMixin, WorkItemStateTransitionsResolverMixin
from polaris.analytics.service.graphql.work_item.selectable import WorkItemNode, WorkItemStateTransitionNodes
from polaris.analytics.service.graphql.selectable_field_mixins import SelectablePropertyResolverMixin


class WorkItemStateTransitionsField(graphene.ObjectType):
    class Meta:
        interfaces = (WorkItemStateTransitions,)



class WorkItem(
    # interface resolver mixins
    WorkItemInfoResolverMixin,
    SelectablePropertyResolverMixin,
    # selectable
    Selectable
):
    class Meta:
        interfaces = (WorkItemInfo, WorkItemsSourceRef)
        named_node_resolver = WorkItemNode
        interface_resolvers = {}
        selectable_field_resolvers = {
            'work_item_state_transitions': WorkItemStateTransitionNodes
        }
        connection_class = lambda: WorkItems



    @classmethod
    def resolve_field(cls, parent, info, key, **kwargs):
        return cls.resolve_instance(key=key, **kwargs)

    # local properties
    work_item_state_transitions = graphene.Field(graphene.List(WorkItemStateTransitionsField))

    def resolve_work_item_state_transitions(self, info, **kwargs):
        return self.resolve_selectable_field('work_item_state_transitions')

class WorkItems(
    CountableConnection
):
    class Meta:
        node = WorkItem


class WorkItemsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):

    work_items = WorkItem.ConnectionField()

    def resolve_work_items(self, info, **kwargs):
        return WorkItem.resolve_connection(
            self.get_connection_resolver_context('work_items'),
            self.get_connection_node_resolver('work_items'),
            self.get_instance_query_params(),
            **kwargs
        )