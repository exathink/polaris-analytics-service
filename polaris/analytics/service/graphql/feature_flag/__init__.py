# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from flask_security import current_user

from polaris.graphql.exceptions import AccessDeniedException
from polaris.graphql.selectable import Selectable, ConnectionResolverMixin
from polaris.graphql.interfaces import NamedNode
from .selectable import FeatureFlagNode, FeatureFlagFeatureFlagEnablements, FeatureFlagScopeRefInfo, \
    ScopedFeatureFlagsNodes, AllFeatureFlagNodes
from ..interface_mixins import NamedNodeResolverMixin, KeyIdResolverMixin
from polaris.graphql.connection_utils import CountableConnection
from ..interfaces import FeatureFlagInfo, FeatureFlagEnablements, Enablement, FeatureFlagEnablementDetail


class FeatureFlag(
    # Interface Mixins
    NamedNodeResolverMixin,
    Selectable
):
    class Meta:
        interfaces = (NamedNode, FeatureFlagInfo, Enablement, FeatureFlagEnablements)
        named_node_resolver = FeatureFlagNode
        interface_resolvers = {
            'FeatureFlagEnablements': FeatureFlagFeatureFlagEnablements,
            'FeatureFlagScopeRef': FeatureFlagScopeRefInfo
        }
        connection_class = lambda: FeatureFlags

    def __init__(self, *args, **kwargs):
        self.enablements = None
        super().__init__(*args, **kwargs)

    @classmethod
    def resolve_field(cls, info, **kwargs):
        return cls.resolve_instance(**kwargs)

    @classmethod
    def resolve_all_feature_flags(cls, info, **kwargs):
        return cls.resolve_connection(
            'all_feature_flags',
            AllFeatureFlagNodes,
            params=None,
            **kwargs
        )

    def resolve_enablements(self, info, **kwargs):
        return [FeatureFlagEnablementDetail(**enablement) for enablement in self.enablements]


class FeatureFlags(
    CountableConnection
):
    class Meta:
        node = FeatureFlag


class FeatureFlagsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):
    feature_flags = FeatureFlag.ConnectionField()

    def resolve_feature_flags(self, info, **kwargs):
        return FeatureFlag.resolve_connection(
            self.get_connection_resolver_context('feature_flags'),
            self.get_connection_node_resolver('feature_flags'),
            self.get_instance_query_params(),
            **kwargs
        )
