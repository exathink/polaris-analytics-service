# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene

from polaris.graphql.selectable import Selectable, ConnectionResolverMixin
from polaris.graphql.connection_utils import CountableConnection
from polaris.graphql.utils import create_tuple, init_tuple

from ..interfaces import CommitInfo, CommitChangeStats, FileTypesSummary, \
    WorkItemsSummary
from ..interface_mixins import KeyIdResolverMixin

from .selectables import CommitNode, CommitFileTypesSummary


class CommitInfoResolverMixin(KeyIdResolverMixin):
    commit_tuple = create_tuple(CommitInfo)
    stats_tuple = create_tuple(CommitChangeStats)
    no_stats = dict(files=0, lines=0, insertions=0, deletions=0)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commit_info = init_tuple(self.commit_tuple, **kwargs)

    def resolve_stats(self, info, **kwargs):
        return init_tuple(self.stats_tuple, **(self.commit_info.stats or self.no_stats))

    def resolve_file_types_summary(self, info, **kwargs):
        file_types_summary = self.commit_info.file_types_summary or []
        return [FileTypesSummary(**summary) for summary in file_types_summary]

    def resolve_work_items_summaries(self, info, **kwargs):
        work_items_summaries = self.commit_info.work_items_summaries or []
        return [WorkItemsSummary(**summary) for summary in work_items_summaries]


class Commit(
    # interface mixins
    CommitInfoResolverMixin,
    #
    Selectable
):
    class Meta:
        interfaces = (CommitInfo,)
        named_node_resolver = CommitNode
        interface_resolvers = {}
        connection_class = lambda: Commits

    @classmethod
    def key_to_instance_resolver_params(cls, key):
        key_parts = key.split(':')
        assert len(key_parts) == 2
        return dict(repository_key=key_parts[0], commit_key=key_parts[1])

    @classmethod
    def resolve_field(cls, info, commit_key, **kwargs):
        return cls.resolve_instance(commit_key, **kwargs)


class Commits(
    CountableConnection
):
    class Meta:
        node = Commit


class CommitsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):
    commits = Commit.ConnectionField(
        before=graphene.Argument(
            graphene.DateTime, required=False,
            description='show commits strictly before this timestamp. '
        ),
        days=graphene.Argument(
            graphene.Int,
            required=False,
            description="Return commits within the specified number of days. "
                        "If before is specified, it returns commits with commit dates"
                        "between (before - days) and before"
                        "If before is not specified the it returns commits for the"
                        "previous n days starting from utc now"
        ),
        nospecs_only=graphene.Argument(
            graphene.Boolean,
            required=False,
            default_value=False,
            description="Return only commits that have no work items associated"
        )
    )

    def resolve_commits(self, info, **kwargs):
        return Commit.resolve_connection(
            self.get_connection_resolver_context('commits'),
            self.get_connection_node_resolver('commits'),
            self.get_instance_query_params(),
            **kwargs
        )
