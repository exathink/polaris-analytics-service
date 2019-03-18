# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.analytics.service.graphql.interface_mixins import KeyIdResolverMixin, WorkItemInfoResolverMixin
from polaris.analytics.service.graphql.interfaces import WorkItemInfo, \
    WorkItemsSourceRef, WorkItemStateTransition,\
    WorkItemCommitInfo

from polaris.analytics.service.graphql.work_item.selectable import \
    WorkItemNode, WorkItemEventNodes, WorkItemCommitNodes, WorkItemEventNode

from polaris.graphql.selectable import ConnectionResolverMixin
from polaris.graphql.selectable import CountableConnection
from polaris.graphql.selectable import Selectable
from ..interface_mixins import KeyIdResolverMixin
from ..commit import CommitsConnectionMixin


class WorkItemEvent(
    # interface mixins
    KeyIdResolverMixin,

    Selectable
):
    class Meta:
        interfaces = (WorkItemInfo, WorkItemStateTransition, WorkItemsSourceRef)
        named_node_resolver = WorkItemEventNode
        interface_resolvers = {}
        connection_class = lambda: WorkItemEvents

    @classmethod
    def key_to_instance_resolver_params(cls, key):
        key_parts = key.split(':')
        assert len(key_parts) == 2
        return dict(work_item_key=key_parts[0], seq_no=key_parts[1])

    @classmethod
    def resolve_field(cls, info, work_item_event_key, **kwargs):
        return cls.resolve_instance(work_item_event_key, **kwargs)


class WorkItemEvents(
    CountableConnection
):
    class Meta:
        node = WorkItemEvent


class WorkItemEventsConnectionMixin(ConnectionResolverMixin):

    work_item_events = WorkItemEvent.ConnectionField(
        before=graphene.Argument(
            graphene.DateTime, required=False,
            description='show events whose eventDate is strictly before this timestamp. '
        ),
        days=graphene.Argument(
            graphene.Int,
            required=False,
            description="Return events with eventDate within the specified number of days. "
                        "If before is specified, it returns events with eventDate"
                        "between (before - days) and before"
                        "If before is not specified the it returns events for the"
                        "previous n days starting from utc now"
        )
    )

    def resolve_work_item_events(self, info, **kwargs):
        return WorkItemEvent.resolve_connection(
            self.get_connection_resolver_context('work_item_events'),
            self.get_connection_node_resolver('work_item_events'),
            self.get_instance_query_params(),
            **kwargs
        )


class WorkItemCommit(
    Selectable
):
    class Meta:
        interfaces = (WorkItemInfo, WorkItemCommitInfo, WorkItemsSourceRef)
        named_node_resolver = None
        interface_resolvers = {}
        connection_class = lambda: WorkItemCommits


class WorkItemCommits(
    CountableConnection
):
    class Meta:
        node = WorkItemCommit


class WorkItemCommitsConnectionMixin(ConnectionResolverMixin):

    work_item_commits = WorkItemCommit.ConnectionField(
        before=graphene.Argument(
            graphene.DateTime, required=False,
            description='show commit whose eventDate is strictly before this timestamp. '
        ),
        days=graphene.Argument(
            graphene.Int,
            required=False,
            description="Return events with eventDate within the specified number of days. "
                        "If before is specified, it returns events with eventDate"
                        "between (before - days) and before"
                        "If before is not specified the it returns events for the"
                        "previous n days starting from utc now"
        )
    )

    def resolve_work_item_commits(self, info, **kwargs):
        return WorkItemCommit.resolve_connection(
            self.get_connection_resolver_context('work_item_commits'),
            self.get_connection_node_resolver('work_item_commits'),
            self.get_instance_query_params(),
            **kwargs
        )


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


