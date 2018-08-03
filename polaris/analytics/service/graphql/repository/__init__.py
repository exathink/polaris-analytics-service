# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from ..interfaces import NamedNode, CommitSummary, ContributorSummary
from ..mixins import NamedNodeResolverMixin, CommitSummaryResolverMixin, ContributorSummaryResolverMixin
from .selectables import RepositoryNode, RepositoriesCommitSummary, RepositoriesContributorSummary

from polaris.graphql.connection_utils import QueryConnectionField


class Repository(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorSummaryResolverMixin,
    graphene.ObjectType
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary)

    NamedNodeResolver = RepositoryNode
    InterfaceResolvers = {
        'CommitSummary': RepositoriesCommitSummary,
        'ContributorSummary': RepositoriesContributorSummary
    }
    InterfaceEnum = graphene.Enum(
        'RepositoryInterfaces', [
            ('CommitSummary', 'CommitSummary'),
            ('ContributorSummary', 'ContributorSummary')
        ]
    )

    @classmethod
    def Field(cls):
        return graphene.Field(
            cls,
            key=graphene.Argument(type=graphene.String, required=True),
            interfaces=graphene.Argument(
                graphene.List(cls.InterfaceEnum),
                required=False,
            )
        )

    @classmethod
    def ConnectionField(cls, **kwargs):
        return QueryConnectionField(
            Repositories,
            interfaces=graphene.Argument(
                graphene.List(cls.InterfaceEnum),
                required=False,
            ),
            **kwargs
        )

    @classmethod
    def resolve_field(cls, parent, info, repository_key, **kwargs):
        return cls.resolve_instance(params=dict(repository_key=repository_key), **kwargs)

    def get_node_query_params(self):
        return dict(repository_key=self.key)


class Repositories(graphene.relay.Connection):
    class Meta:
        node = Repository
