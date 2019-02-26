# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.analytics.service.graphql.interface_mixins import KeyIdResolverMixin, WorkItemInfoResolverMixin
from polaris.analytics.service.graphql.interfaces import WorkItemInfo, WorkItemsSourceRef
from polaris.analytics.service.graphql.work_item.selectable import \
    WorkItemNode, WorkItemEventNodes, WorkItemCommitNodes

from polaris.graphql.selectable import ConnectionResolverMixin
from polaris.graphql.selectable import CountableConnection
from polaris.graphql.selectable import Selectable
from ..commit import CommitsConnectionMixin
from ..work_item_event import WorkItemEventsConnectionMixin


class WorkItem(
    # interface resolver mixins
    WorkItemInfoResolverMixin,
    WorkItemEventsConnectionMixin,
    CommitsConnectionMixin,
    # selectable
    Selectable
):
    class Meta:
        interfaces = (WorkItemInfo, WorkItemsSourceRef)
        named_node_resolver = WorkItemNode
        interface_resolvers = {}
        connection_node_resolvers = {
            'work_item_events': WorkItemEventNodes,
            'commits': WorkItemCommitNodes
        }
        connection_class = lambda: WorkItems



    @classmethod
    def resolve_field(cls, parent, info, key, **kwargs):
        return cls.resolve_instance(key=key, **kwargs)


class WorkItems(
    CountableConnection
):
    class Meta:
        node = WorkItem


class WorkItemsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):

    work_items = WorkItem.ConnectionField(
        before=graphene.Argument(
            graphene.DateTime, required=False,
            description='show work_items whose latest update is strictly before this timestamp. '
        ),
        days=graphene.Argument(
            graphene.Int,
            required=False,
            description="Return work items last updated within the specified number of days. "
                        "If before is specified, it returns work items with commit dates"
                        "between (before - days) and before"
                        "If before is not specified the it returns work items for the"
                        "previous n days starting from utc now"
        )

    )

    def resolve_work_items(self, info, **kwargs):
        return WorkItem.resolve_connection(
            self.get_connection_resolver_context('work_items'),
            self.get_connection_node_resolver('work_items'),
            self.get_instance_query_params(),
            **kwargs
        )


