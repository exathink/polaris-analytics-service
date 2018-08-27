# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.selectable import Selectable
from polaris.graphql.interfaces import NamedNode
from ..interfaces import CommitSummary, ContributorCount, OrganizationRef
from ..interface_mixins import NamedNodeResolverMixin, CommitSummaryResolverMixin, ContributorCountResolverMixin, OrganizationRefResolverMixin

from ..summaries import ActivityLevelSummary, Inceptions
from ..summary_mixins import ActivityLevelSummaryResolverMixin, InceptionsResolverMixin

from .selectables import RepositoryNode, \
    RepositoriesCommitSummary, \
    RepositoryContributorNodes, \
    RepositoriesContributorCount, RepositoriesOrganizationRef

from ..contributor import Contributor

from polaris.graphql.connection_utils import CountableConnection, QueryConnectionField


class Repository(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorCountResolverMixin,
    OrganizationRefResolverMixin,
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorCount, OrganizationRef)
        named_node_resolver = RepositoryNode
        interface_resolvers = {
            'CommitSummary': RepositoriesCommitSummary,
            'ContributorCount': RepositoriesContributorCount,
            'OrganizationRef': RepositoriesOrganizationRef
        }
        connection_class = lambda: Repositories

    # Child Fields
    contributors = Contributor.ConnectionField()

    @classmethod
    def resolve_field(cls, parent, info, repository_key, **kwargs):
        return cls.resolve_instance(repository_key, **kwargs)

    def resolve_contributors(self, info, **kwargs):
        return Contributor.resolve_connection(
            'repository_contributors',
            RepositoryContributorNodes,
            self.get_instance_query_params(),
            level_of_detail='repository',
            apply_distinct=True,
            **kwargs
        )


class Repositories(
    ActivityLevelSummaryResolverMixin,
    InceptionsResolverMixin,
    CountableConnection
):
    class Meta:
        node = Repository
        summaries = (ActivityLevelSummary, graphene.List(Inceptions))
