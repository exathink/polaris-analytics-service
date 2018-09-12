# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene

from polaris.graphql.selectable import Selectable, ConnectionResolverMixin
from polaris.graphql.connection_utils import CountableConnection

from polaris.graphql.interfaces import NamedNode

from ..interface_mixins import NamedNodeResolverMixin

from .selectables import CommitNode


class Commit(
    # interface mixins
    NamedNodeResolverMixin,
    #
    Selectable
):
    class Meta:
        interfaces = (NamedNode,)
        named_node_resolver = CommitNode
        interface_resolvers = {}

    @classmethod
    def resolve_field(cls, info, commit_key, **kwargs):
        return cls.resolve_instance(commit_key, **kwargs)


class Commits(
    CountableConnection
):
    class Meta:
        node = Commit

