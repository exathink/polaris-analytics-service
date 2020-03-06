# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from polaris.analytics.service.graphql.interfaces import FeatureFlagInfo
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver, ConnectionResolver
from polaris.analytics.db.model import feature_flags, feature_flag_enablements, accounts
from ..interfaces import \
    FeatureFlagInfo, FeatureFlagEnablementDetail, FeatureFlagEnablements, \
    Enablement
from polaris.graphql.base_classes import ConnectionResolver
from polaris.analytics.db.enums import FeatureFlagScope

from sqlalchemy import select, bindparam, func, case, and_, union, alias

from polaris.auth.db.model import users
from polaris.utils.exceptions import ProcessingException


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


class FeatureFlagFeatureFlagEnablements(InterfaceResolver):
    interface = FeatureFlagEnablements

    @staticmethod
    def interface_selector(feature_flag_nodes, **kwargs):
        user_enablements = select([
            feature_flag_enablements.c.feature_flag_id,
            feature_flag_enablements.c.scope_key,
            func.concat(users.c.first_name, ' ', users.c.last_name).label('scope_ref_name'),
        ]).select_from(
            feature_flag_enablements.outerjoin(
                users, users.c.key == feature_flag_enablements.c.scope_key
            )
        ).where(feature_flag_enablements.c.scope == FeatureFlagScope.user.value)

        account_enablements = select([
            feature_flag_enablements.c.feature_flag_id,
            feature_flag_enablements.c.scope_key,
            accounts.c.name.label('scope_ref_name')
        ]).select_from(
            feature_flag_enablements.outerjoin(
                accounts, accounts.c.key == feature_flag_enablements.c.scope_key
            )
        ).where(feature_flag_enablements.c.scope == FeatureFlagScope.account.value)

        enablement_scope_refs = union(user_enablements, account_enablements).alias()

        return select([
            feature_flag_nodes.c.id,
            func.json_agg(
                func.json_build_object(
                    'scope_key', feature_flag_enablements.c.scope_key,
                    'scope', feature_flag_enablements.c.scope,
                    'enabled', case(
                        [
                            (feature_flag_nodes.c.active == False, False),
                            (feature_flag_nodes.c.enable_all == True, True)
                        ],
                        else_=feature_flag_enablements.c.enabled
                    ),
                    'scope_ref_name', enablement_scope_refs.c.scope_ref_name
                )
            ).label('enablements')

        ]).select_from(
            feature_flag_nodes.outerjoin(
                feature_flag_enablements, feature_flag_nodes.c.id == feature_flag_enablements.c.feature_flag_id
            ).outerjoin(
                enablement_scope_refs, and_(
                    enablement_scope_refs.c.feature_flag_id == feature_flag_enablements.c.feature_flag_id,
                    enablement_scope_refs.c.scope_key == feature_flag_enablements.c.scope_key
                )
            )
        ).group_by(
            feature_flag_nodes.c.id
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
            ]).where(feature_flags.c.active == True)
        else:
            return select([
                feature_flags.c.id,
                feature_flags.c.key,
                feature_flags.c.name,
                feature_flags.c.enable_all,
                feature_flags.c.active,
                feature_flags.c.created
            ])

    @staticmethod
    def sort_order(all_feature_flag_nodes, **kwargs):
        return [all_feature_flag_nodes.c.created.desc().nullslast()]


class ScopedFeatureFlagsNodes(ConnectionResolver):
    interfaces = (NamedNode, FeatureFlagInfo, Enablement)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        if kwargs.get('scope_key'):
            return select(
                [
                    feature_flags.c.id,
                    feature_flags.c.key,
                    feature_flags.c.name,
                    feature_flags.c.active,
                    feature_flags.c.created,
                    feature_flags.c.enable_all,
                    case(
                        [
                            (feature_flags.c.enable_all, True),
                            (feature_flag_enablements.c.enabled == None, None)
                        ],
                        else_=feature_flag_enablements.c.enabled
                    ).label('enabled')
                ]
            ).select_from(
                feature_flags.outerjoin(
                    feature_flag_enablements,
                    and_(
                        feature_flag_enablements.c.feature_flag_id == feature_flags.c.id,
                        feature_flag_enablements.c.scope_key == kwargs.get('scope_key')
                    )
                )
            ).where(
                feature_flags.c.active
            )
        else:
            raise ProcessingException('ScopedFeatureFlagNodes requires the scope_key keyword arg to be provided.')
