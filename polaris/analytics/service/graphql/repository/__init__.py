# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from .selectables import RepositoryNode, RepositoriesCommitSummary, RepositoriesContributorSummary
from ..interfaces import NamedNode, CommitSummary, ContributorSummary
from ..mixins import CommitSummaryResolverMixin, ContributorSummaryResolverMixin
from ..utils import resolve_instance


class Repository(
    CommitSummaryResolverMixin,
    ContributorSummaryResolverMixin,
    graphene.ObjectType
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary )

    @classmethod
    def Field(cls):
        return graphene.Field(
            cls,
            key=graphene.Argument(type=graphene.String, required=True),
            interfaces=graphene.Argument(
                graphene.List(graphene.Enum(
                    'RepositoryInterfaces', [
                        ('CommitSummary', 'CommitSummary'),
                        ('ContributorSummary', 'ContributorSummary')
                    ]
                )),
                required=False,
            )
        )

    InterfaceResolvers = {
        'NamedNode': RepositoryNode,
        'CommitSummary': RepositoriesCommitSummary,
        'ContributorSummary': RepositoriesContributorSummary
    }

    @classmethod
    def resolve_field(cls, parent, info, repository_key, **kwargs):
        return resolve_instance(
            cls.InterfaceResolvers,
            resolver_context='repository',
            params=dict(repository_key=repository_key),
            output_type=cls,
            **kwargs
        )
    

    def get_node_query_params(self):
        return dict(repository_key=self.key)



