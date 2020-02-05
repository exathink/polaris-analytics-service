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

from ..interfaces import CommitInfo,CommitChangeStats, FileTypesSummary, \
    CommitWorkItemsSummary
from ..interface_mixins import KeyIdResolverMixin

from .selectables import CommitNode, CommitFileTypesSummary


class CommitInfoResolverMixin(KeyIdResolverMixin):
    commit_tuple = create_tuple(CommitInfo)
    stats_tuple = create_tuple(CommitChangeStats)
    no_stats = dict(files=0, lines=0, insertions=0, deletions=0)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commit_info = init_tuple(self.commit_tuple, **kwargs)

    def resolve_commit(self, info, **kwargs):
        return self.resolve_interface_for_instance(
            interface=['Commit'],
            params=self.get_instance_query_params(),
            **kwargs
        )

    def get_commit(self, info, **kwargs):
        if self.commit_info is None:
            self.commit_info = self.resolve_commit(info, **kwargs)
        return self.commit_info

    def resolve_commit_hash(self, info, **kwargs):
        return self.get_commit(info, **kwargs).commit_hash

    def resolve_repository(self, info, **kwargs):
        return self.get_commit(info, **kwargs).repository

    def resolve_repository_key(self, info, **kwargs):
        return self.get_commit(info, **kwargs).repository_key

    def resolve_repository_url(self, info, **kwargs):
        return self.get_commit(info, **kwargs).repository_url

    def resolve_commit_date(self, info, **kwargs):
        return self.get_commit(info, **kwargs).commit_date

    def resolve_committer(self, info, **kwargs):
        return self.get_commit(info, **kwargs).committer

    def resolve_committer_key(self, info, **kwargs):
        return self.get_commit(info, **kwargs).committer_key

    def resolve_author_date(self, info, **kwargs):
        return self.get_commit(info, **kwargs).author_date

    def resolve_author(self, info, **kwargs):
        return self.get_commit(info, **kwargs).author

    def resolve_author_key(self, info, **kwargs):
        return self.get_commit(info, **kwargs).author_key

    def resolve_commit_message(self, info, **kwargs):
        return self.get_commit(info, **kwargs).commit_message

    def resolve_num_parents(self, info, **kwargs):
        return self.get_commit(info, **kwargs).num_parents

    def resolve_stats(self, info, **kwargs):
        return init_tuple(self.stats_tuple, **(self.get_commit(info, **kwargs).stats or self.no_stats))

    def resolve_file_types_summary(self, info, **kwargs):
        file_types_summary = self.get_commit(info, **kwargs).file_types_summary or []
        return [FileTypesSummary(**summary) for summary in file_types_summary]

    def resolve_work_items_summaries(self, info, **kwargs):
        work_items_summaries = self.get_commit(info, **kwargs).work_items_summaries or []
        return [CommitWorkItemsSummary(**summary) for summary in work_items_summaries]


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
        )
    )

    def resolve_commits(self, info, **kwargs):
        return Commit.resolve_connection(
            self.get_connection_resolver_context('commits'),
            self.get_connection_node_resolver('commits'),
            self.get_instance_query_params(),
            **kwargs
        )
