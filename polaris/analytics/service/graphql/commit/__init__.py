# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene

from polaris.graphql.selectable import Selectable, ConnectionResolverMixin
from polaris.graphql.connection_utils import CountableConnection


from ..interfaces import CommitInfo

from ..interface_mixins import CommitInfoResolverMixin

from .selectables import CommitNode


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

