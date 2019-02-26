# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.graphql.selectable import Selectable
from polaris.graphql.selectable import CountableConnection
from polaris.analytics.service.graphql.interfaces import WorkItemInfo, WorkItemStateTransition
from polaris.graphql.selectable import ConnectionResolverMixin


class WorkItemEvent(
    Selectable
):
    class Meta:
        interfaces = (WorkItemInfo, WorkItemStateTransition)
        named_node_resolver = None
        interface_resolvers = {}
        connection_class = lambda: WorkItemEvents


class WorkItemEvents(
    CountableConnection
):
    class Meta:
        node = WorkItemEvent


class WorkItemEventsConnectionMixin(ConnectionResolverMixin):

    work_item_events = WorkItemEvent.ConnectionField()

    def resolve_work_item_events(self, info, **kwargs):
        return WorkItemEvent.resolve_connection(
            self.get_connection_resolver_context('work_item_events'),
            self.get_connection_node_resolver('work_item_events'),
            self.get_instance_query_params(),
            **kwargs
        )