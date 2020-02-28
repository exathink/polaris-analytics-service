# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.graphql.interfaces import NamedNode
from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver
from polaris.analytics.db.model import feature_flags, feature_flag_enablements
from ..interfaces import FeatureFlagInfo, FeatureFlagEnablementInfo, FeatureFlagScopeRef
from polaris.graphql.base_classes import ConnectionResolver

from sqlalchemy import select, bindparam, func, case, and_

from polaris.auth.db.model import users


class FeatureFlagNode(NamedNodeResolver):
    interface = NamedNode

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            feature_flags.c.id,
            feature_flags.c.key,
            feature_flags.c.name,
            feature_flags.c.enable_all
        ]).select_from(
            feature_flags
        ).where(
            feature_flags.c.key == bindparam('key')
        )

# class FeatureFlagNodeInfo(InterfaceResolver):
#     interface = FeatureFlagInfo
#
#     @staticmethod
#     def interface_selector(feature_flag_nodes, **kwargs):
#         return select([
#             feature_flag_nodes.c.enable_all,
#             feature_flag_nodes.c.created,
#             feature_flag_nodes.c.updated
#         ])


class FeatureFlagEnablementNodeInfo(InterfaceResolver):
    interface = FeatureFlagEnablementInfo

    @staticmethod
    def interface_selector(feature_flag_nodes, **kwargs):
        if kwargs.get('scope_key') is not None:
            return select([
                feature_flag_nodes.c.id,
                func.coalesce(feature_flag_enablements.c.scope, kwargs.get('scope')).label('scope'),
                func.coalesce(feature_flag_enablements.c.scope_key, kwargs.get('scope_key')).label('scope_key'),
                case(
                    [
                        (feature_flag_nodes.c.enable_all, True),
                    ],
                    else_=func.coalesce(feature_flag_enablements.c.enabled, False)
                ).label('enabled')
            ]).select_from(
                feature_flag_nodes.outerjoin(
                    feature_flag_enablements,
                    and_(
                        feature_flag_enablements.c.feature_flag_id == feature_flag_nodes.c.id,
                        feature_flag_enablements.c.scope == kwargs.get('scope'),
                        feature_flag_enablements.c.scope_key == kwargs.get('scope_key')
                    )
                )
            )
        else:
            return select([
                feature_flag_nodes.c.id,
                feature_flag_nodes.c.enable_all,
                feature_flag_enablements.c.scope,
                feature_flag_enablements.c.scope_key,
                feature_flag_enablements.c.enabled
            ]).select_from(
                feature_flag_enablements.join(
                    feature_flag_nodes, feature_flag_nodes.c.id == feature_flag_enablements.c.feature_flag_id
                )
            )


class FeatureFlagScopeRefInfo(InterfaceResolver):
    interface = FeatureFlagScopeRef

    @staticmethod
    def interface_selector(feature_flag_nodes, **kwargs):
        return select([
            feature_flag_nodes.c.id,
            func.concat(users.c.first_name, ' ', users.c.last_name).label('scope_ref_name'),
            users.c.first_name,
            users.c.last_name,
            feature_flag_enablements.c.scope_key
        ]).select_from(
            feature_flag_enablements.outerjoin(users, feature_flag_enablements.c.scope_key == users.c.key)
        ).where(
            feature_flag_nodes.c.id == feature_flag_enablements.c.feature_flag_id
        )


class AllFeatureFlagNodes(ConnectionResolver):
    interfaces = (NamedNode, FeatureFlagInfo)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        if kwargs.get('active_only') is True:
            return select([
                feature_flags.c.id,
                feature_flags.c.key,
                feature_flags.c.name,
                feature_flags.c.enable_all,
                feature_flags.c.active,
                feature_flags.c.created
            ]).where(feature_flags.c.active==True)
        else:
            return select([
                feature_flags.c.id,
                feature_flags.c.key,
                feature_flags.c.name,
                feature_flags.c.enable_all,
                feature_flags.c.active,
                feature_flags.c.created
            ])

    # @staticmethod
    # def sort_order(all_feature_flag_nodes, **kwargs):
    #     return [all_feature_flag_nodes.c.created.desc().nullslast()]
