# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from polaris.graphql.mixins import NamedNodeResolverMixin
from polaris.analytics.service.graphql.pull_request.selectable import PullRequestNode
from polaris.graphql.selectable import Selectable
from polaris.graphql.interfaces import NamedNode

class PullRequest(
    # Interface resolver mixin
    NamedNodeResolverMixin,

    # selectable
    Selectable
):
    class Meta:
        interfaces = (NamedNode,)
        named_node_resolver = PullRequestNode
        interface_resolvers = {}
        connection_node_resolvers = {}

    @classmethod
    def resolve_field(cls, parent, info, key, **kwargs):
        return cls.resolve_instance(key=key, **kwargs)