# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from ..interface_mixins import NamedNodeResolverMixin
from polaris.graphql.selectable import Selectable, CountableConnection, ConnectionResolverMixin
from polaris.graphql.interfaces import NamedNode
from polaris.analytics.service.graphql.interfaces import WorkTrackingIntegrationType

from .selectable import WorkItemsSourceNode, WorkItemsSourceWorkItemNodes, WorkItemsSourceWorkItemEventNodes, \
    WorkItemsSourceWorkItemCommitNodes, WorkItemsSourceDistinctStates
from ..work_item import WorkItemsConnectionMixin, WorkItemEventsConnectionMixin, WorkItemCommitsConnectionMixin
from ..selectable_field_mixins import DistinctStatesResolverMixin


class WorkItemsSource(
    # interface mixins
    NamedNodeResolverMixin,

    # connection mixins
    WorkItemsConnectionMixin,
    WorkItemEventsConnectionMixin,
    WorkItemCommitsConnectionMixin,

    # Field mixins
    DistinctStatesResolverMixin,
    #
    Selectable,
):
    class Meta:
        interfaces = (NamedNode,)
        interface_resolvers = {}
        named_node_resolver = WorkItemsSourceNode

        connection_node_resolvers = {
            'work_items': WorkItemsSourceWorkItemNodes,
            'work_item_events': WorkItemsSourceWorkItemEventNodes,
            'work_item_commits': WorkItemsSourceWorkItemCommitNodes
        }

        selectable_field_resolvers = {
            'distinct_states': WorkItemsSourceDistinctStates
        }
        connection_class = lambda: WorkItemsSources

    @classmethod
    def resolve_field(cls, parent, info, work_items_source_key, **kwargs):
        return cls.resolve_instance(key=work_items_source_key, **kwargs)


class WorkItemsSources(
    CountableConnection
):
    class Meta:
        node = WorkItemsSource


class WorkItemsSourcesConnectionMixin(ConnectionResolverMixin):
    work_items_sources = WorkItemsSource.ConnectionField(
        integration_type=graphene.Argument(
            WorkTrackingIntegrationType, required=False,
            description='filter the work items sources by integration type'
        ),
    )

    def resolve_work_items_sources(self, info, **kwargs):
        return WorkItemsSource.resolve_connection(
            self.get_connection_resolver_context('work_items_sources'),
            self.get_connection_node_resolver('work_items_sources'),
            self.get_instance_query_params(),
            **kwargs
        )
