# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.selectable import Selectable
from polaris.graphql.interfaces import NamedNode
from ..interfaces import CommitSummary, ContributorSummary
from ..mixins import NamedNodeResolverMixin, CommitSummaryResolverMixin, ContributorSummaryResolverMixin
from .selectables import RepositoryNode, RepositoriesCommitSummary, RepositoriesContributorSummary

from polaris.graphql.connection_utils import CountableConnection, QueryConnectionField


class Repository(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorSummaryResolverMixin,
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary)
        named_node_resolver = RepositoryNode
        interface_resolvers = {
            'CommitSummary': RepositoriesCommitSummary,
            'ContributorSummary': RepositoriesContributorSummary
        }
        connection_class = lambda: Repositories


    @classmethod
    def resolve_field(cls, parent, info, repository_key, **kwargs):
        return cls.resolve_instance(repository_key, **kwargs)


class Repositories(CountableConnection):
    class Meta:
        node = Repository
