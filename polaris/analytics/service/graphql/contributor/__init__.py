# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from polaris.graphql.connection_utils import CountableConnection
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable, ConnectionResolverMixin
from .selectables import \
    ContributorNodes, ContributorCommitNodes, ContributorRepositoriesNodes,\
    ContributorsCommitSummary, ContributorsRepositoryCount, ContributorRecentlyActiveRepositoriesNodes, \
    ContributorCumulativeCommitCount

from ..interfaces import CommitSummary, RepositoryCount
from ..interface_mixins import KeyIdResolverMixin, \
    NamedNodeResolverMixin, CommitSummaryResolverMixin, RepositoryCountResolverMixin

from ..summaries import ActivityLevelSummary, InceptionsSummary
from ..summary_mixins import \
    ActivityLevelSummaryResolverMixin, \
    InceptionsResolverMixin

from ..commit import CommitsConnectionMixin
from .selectable_fields import \
    ContributorRepositoriesActivitySummaryResolverMixin, \
    ContributorRecentlyActiveRepositoriesResolverMixin

from ..selectable_field_mixins import CumulativeCommitCountResolverMixin

from .mutations import UpdateContributorForContributorAliases


# Mutations
class ContributorMutationsMixin:
    update_contributor_for_contributor_aliases = UpdateContributorForContributorAliases.Field()


class Contributor(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    RepositoryCountResolverMixin,
    # connection mixins
    CommitsConnectionMixin,
    # Selectable fields
    ContributorRepositoriesActivitySummaryResolverMixin,
    ContributorRecentlyActiveRepositoriesResolverMixin,
    CumulativeCommitCountResolverMixin,
    #
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, RepositoryCount)
        named_node_resolver = ContributorNodes
        interface_resolvers = {
            'CommitSummary': ContributorsCommitSummary,
            'RepositoryCount': ContributorsRepositoryCount
        }
        selectable_field_resolvers = {
            'repositories_activity_summary': ContributorRepositoriesNodes,
            'recently_active_repositories' : ContributorRecentlyActiveRepositoriesNodes,
            'cumulative_commit_count': ContributorCumulativeCommitCount,
        }
        connection_node_resolvers = {
            'commits': ContributorCommitNodes
        }
        connection_class = lambda: Contributors



    @classmethod
    def resolve_field(cls, parent, info, key, **kwargs):
        return cls.resolve_instance(key, **kwargs)


class Contributors(
    ActivityLevelSummaryResolverMixin,
    InceptionsResolverMixin,
    CountableConnection
):
    class Meta:
        node = Contributor
        summaries = (ActivityLevelSummary, graphene.List(InceptionsSummary))


class ContributorsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):

    contributors = Contributor.ConnectionField()

    def resolve_contributors(self, info, **kwargs):
        return Contributor.resolve_connection(
            self.get_connection_resolver_context('contributors'),
            self.get_connection_node_resolver('contributors'),
            self.get_instance_query_params(),
            level_of_detail='repository',
            apply_distinct=True,
            **kwargs
        )


class RecentlyActiveContributorsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):
    recently_active_contributors = Contributor.ConnectionField(
        before=graphene.Argument(
            graphene.DateTime,
            required=False,
            description="End date of period to search for activity. If not specified it defaults to utc now"
        ),
        days=graphene.Argument(
            graphene.Int,
            required=False,
            default_value=7,
            description="Return contributors with commits within the specified number of days"
        )
    )

    def resolve_recently_active_contributors(self, info, **kwargs):
        return Contributor.resolve_connection(
            self.get_connection_resolver_context('recently_active_contributors'),
            self.get_connection_node_resolver('recently_active_contributors'),
            self.get_instance_query_params(),
            **kwargs
        )


