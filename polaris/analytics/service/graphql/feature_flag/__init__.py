# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.selectable import Selectable
from polaris.graphql.interfaces import NamedNode
from .selectable import FeatureFlagNode
from ..interface_mixins import NamedNodeResolverMixin


class FeatureFlag(
    # Interface Mixins
    NamedNodeResolverMixin,

    Selectable
):
    class Meta:
        interfaces = (NamedNode,)
        named_node_resolver = FeatureFlagNode
        interface_resolvers = {}

    @classmethod
    def resolve_field(cls, info, **kwargs):
        return cls.resolve_instance(**kwargs)
