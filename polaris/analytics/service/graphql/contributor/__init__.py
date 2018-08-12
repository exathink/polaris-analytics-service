# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.graphql.connection_utils import CountableConnection
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable
from .selectables import ContributorNodes, ContributorsCommitSummary, ContributorsRepositoryCount
from ..interfaces import CommitSummary, RepositoryCount
from ..mixins import NamedNodeResolverMixin, CommitSummaryResolverMixin, RepositoryCountResolverMixin


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
    def resolve_field(cls, info, key, **kwargs):
        return cls.resolve_instance(key, **kwargs)


class Contributors(CountableConnection):
    class Meta:
        node = Contributor