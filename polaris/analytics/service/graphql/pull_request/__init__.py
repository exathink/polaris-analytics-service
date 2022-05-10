# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import graphene
from polaris.analytics.service.graphql.pull_request.selectable import PullRequestNode, PullRequestBranchRef, \
    PullRequestWorkItemsSummaries
from polaris.graphql.selectable import Selectable, ConnectionResolverMixin
from polaris.graphql.interfaces import NamedNode
from ..interface_mixins import KeyIdResolverMixin, NamedNodeResolverMixin, WorkItemsSummariesResolverMixin
from polaris.graphql.connection_utils import CountableConnection
from polaris.analytics.service.graphql.interfaces import PullRequestInfo, BranchRef, WorkItemsSummaries


class PullRequest(
    # Interface resolver mixin
    NamedNodeResolverMixin,
    WorkItemsSummariesResolverMixin,

    # selectable
    Selectable
):
    class Meta:
        interfaces = (NamedNode, PullRequestInfo, BranchRef, WorkItemsSummaries)
        named_node_resolver = PullRequestNode
        interface_resolvers = {
            "BranchRef": PullRequestBranchRef,
            "WorkItemsSummaries": PullRequestWorkItemsSummaries

        }
        connection_node_resolvers = {}
        connection_class = lambda: PullRequests

    @classmethod
    def resolve_field(cls, parent, info, key, **kwargs):
        return cls.resolve_instance(key=key, **kwargs)


class PullRequests(
    CountableConnection
):
    class Meta:
        node = PullRequest


class PullRequestsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):
    pull_requests = PullRequest.ConnectionField(
        closed_within_days=graphene.Argument(
            graphene.Int,
            required=False,
            description="Return work items that were closed in the last n days"
        ),
        active_only=graphene.Argument(
            graphene.Boolean,
            required=False,
            description="Return only delivery cycles that are not closed"
        ),
        specs_only=graphene.Boolean(
            required=False,
            description="Only returns metrics for pull requests mapped to a card"
        ),
        before=graphene.Argument(
            graphene.DateTime,
            required=False,
            description="End date of period to search for activity. If not specified it defaults to utc now"
        ),
    )

    def resolve_pull_requests(self, info, **kwargs):
        return PullRequest.resolve_connection(
            self.get_connection_resolver_context('pull_requests'),
            self.get_connection_node_resolver('pull_requests'),
            self.get_instance_query_params(),
            **kwargs
        )
