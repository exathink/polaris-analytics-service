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
from .selectable import FeatureFlagNode, FeatureFlagEnablementNodeInfo, FeatureFlagScopeRefInfo, AllFeatureFlagNodes
from ..interface_mixins import NamedNodeResolverMixin, KeyIdResolverMixin
from polaris.graphql.connection_utils import CountableConnection
from ..interfaces import FeatureFlagEnablementInfo, FeatureFlagScopeRef


class FeatureFlag(
    # Interface Mixins
    NamedNodeResolverMixin,

    Selectable
):
    class Meta:
        interfaces = (NamedNode, FeatureFlagEnablementInfo, FeatureFlagScopeRef)
        named_node_resolver = FeatureFlagNode
        interface_resolvers = {
            'FeatureFlagEnablementInfo': FeatureFlagEnablementNodeInfo,
            'FeatureFlagScopeRef': FeatureFlagScopeRefInfo
        }
        connection_class = lambda: FeatureFlags

    @classmethod
    def resolve_field(cls, info, **kwargs):
        return cls.resolve_instance(**kwargs)

    @classmethod
    def resolve_all_feature_flags(cls, info, **kwargs):
        #if 'admin' in current_user.role_names:
        if True:
            return cls.resolve_connection(
                'all_feature_flags',
                AllFeatureFlagNodes,
                params=None,
                **kwargs
            )
        else:
            raise AccessDeniedException('Access Denied')


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
