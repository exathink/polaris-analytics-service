# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, func, bindparam, and_, or_

from ..interfaces import NamedNode, ScopedRole
from polaris.analytics.db.model import \
    accounts, account_members, \
    organizations, organization_members, feature_flags, feature_flag_enablements
from polaris.graphql.base_classes import ConnectionResolver


class ViewerAccountRoles:
    interfaces = (NamedNode, ScopedRole, )

    @staticmethod
    def selectable(**kwargs):
        return select([
            accounts.c.key,
            accounts.c.name,
            accounts.c.key.label('scope_key'),
            account_members.c.role,
        ]).select_from(
            accounts.join(account_members, accounts.c.id == account_members.c.account_id)
        ).where(account_members.c.user_key == bindparam('key'))


class ViewerOrganizationRoles:
    interfaces = (NamedNode, ScopedRole)

    @staticmethod
    def selectable(**kwargs):
        return select([
            organizations.c.key,
            organizations.c.name,
            organizations.c.key.label('scope_key'),
            organization_members.c.role,
        ]).select_from(
            organizations.join(organization_members, organizations.c.id == organization_members.c.organization_id)
        ).where(organization_members.c.user_key == bindparam('key'))


class ViewerFeatureFlagsNodes(ConnectionResolver):
    interface = NamedNode

    @staticmethod
    def connection_nodes_selector(**kwargs):

        return select([
            feature_flags.c.id,
            feature_flags.c.key,
            feature_flags.c.name,
            feature_flags.c.enable_all,
            feature_flags.c.active

        ]).select_from(
            feature_flags.outerjoin(
                feature_flag_enablements, feature_flag_enablements.c.feature_flag_id==feature_flags.c.id
            )
        ).where(
            and_(
                feature_flags.c.active,
                or_(
                    feature_flags.c.enable_all,
                    and_(
                        feature_flag_enablements.c.scope == kwargs.get('scope'),
                        feature_flag_enablements.c.scope_key == kwargs.get('scope_key')
                    )
                )
            )
        )