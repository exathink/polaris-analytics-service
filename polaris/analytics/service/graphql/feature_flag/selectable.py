# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.graphql.interfaces import NamedNode
from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver
from polaris.analytics.db.model import feature_flags, feature_flag_enablements
from ..interfaces import FeatureFlagEnablementInfo

from sqlalchemy import select, bindparam, func, or_


# from ..interfaces import FeatureFlagInfo


class FeatureFlagNode(NamedNodeResolver):
    interface = NamedNode

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            feature_flags.c.id,
            feature_flags.c.key,
            feature_flags.c.name
        ]).select_from(
            feature_flags
        ).where(
            feature_flags.c.key == bindparam('key')
        )


class FeatureFlagEnablementNodeInfo(InterfaceResolver):
    interface = FeatureFlagEnablementInfo

    @staticmethod
    def interface_selector(feature_flag_nodes, **kwargs):
        return select([
            feature_flag_nodes.c.id,
            feature_flag_enablements.c.scope,
            feature_flag_enablements.c.scope_key,
            func.coalesce(func.coalesce(feature_flag_enablements.c.enabled, \
                                        feature_flag_nodes.c.enable_all), False).label('enabled'),
        ]).select_from(
            feature_flag_nodes.outerjoin(
                feature_flag_enablements, feature_flag_enablements.c.feature_flag_id == feature_flag_nodes.c.id
            )
        ).where(
            or_(
                feature_flag_enablements.c.scope_key == bindparam('key'),
                feature_flag_nodes.c.enable_all == True
            )
        )
