# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from polaris.graphql.connection_utils import CountableConnection
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable
from .selectables import ContributorNodes, ContributorsCommitSummary, ContributorsRepositoryCount

from ..interfaces import CommitSummary, RepositoryCount
from ..interface_mixins import NamedNodeResolverMixin, CommitSummaryResolverMixin, RepositoryCountResolverMixin

from ..summaries import ActivityLevelSummary, InceptionsSummary
from ..summary_mixins import \
    ActivityLevelSummaryResolverMixin, \
    InceptionsResolverMixin


class Contributor(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    RepositoryCountResolverMixin,
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, RepositoryCount)
        named_node_resolver = ContributorNodes
        interface_resolvers = {
            'CommitSummary': ContributorsCommitSummary,
            'RepositoryCount': ContributorsRepositoryCount
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




