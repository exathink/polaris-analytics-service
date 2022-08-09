# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene
from sqlalchemy import select, bindparam, func
from polaris.auth.db.model import users, roles, users_roles
from polaris.analytics.db.model import organizations, organization_members, accounts, account_members
from polaris.graphql.interfaces import KeyIdNode, NamedNode
from ..interfaces import UserInfo, UserRoles


class UserNode:
    interfaces = (NamedNode,)

    @staticmethod
    def selectable(**kwargs):
        return select([
            users.c.id,
            users.c.key,
            func.concat(users.c.first_name, ' ', users.c.last_name).label('name')
        ]).where(
            users.c.key == bindparam('key')
        )


class UserUserInfo:
    interface = UserInfo

    @staticmethod
    def selectable(user_node, **kwargs):
        select_stmt = select([
            users.c.key,
            func.concat(users.c.first_name, ' ', users.c.last_name).label('name'),
            users.c.first_name,
            users.c.last_name,
            users.c.email
        ]).select_from(
            user_node.outerjoin(
                users, user_node.c.key == users.c.key
            )
        )
        if kwargs.get('active_only'):
            return select_stmt.where(
                users.c.active == True
            )
        else:
            return select_stmt


class UserUserRoles:
    interface = UserRoles

    @staticmethod
    def selectable(user_nodes, **kwargs):

        select_system_roles = select([
            user_nodes.c.key,
            func.json_agg(
                roles.c.name
            ).label(
                'system_roles'
            )
        ]).select_from(
            user_nodes.join(
                users, user_nodes.c.key == users.c.key
            ).join(
                users_roles, users_roles.c.user_id == users.c.id
            ).join(
                roles, users_roles.c.role_id == roles.c.id
            )
        ).group_by(
            user_nodes.c.key
        ).cte('system_roles')

        select_organization_roles = select([
            user_nodes.c.key,
            func.json_agg(
                func.json_build_object(
                    'name', organizations.c.name,
                    'scope_key', organizations.c.key,
                    'role', organization_members.c.role
                )
            ).label('organization_roles'),
        ]).select_from(
            user_nodes.join(
                organization_members, user_nodes.c.key == organization_members.c.user_key
            ).join(
                organizations, organization_members.c.organization_id == organizations.c.id
            )
        ).group_by(
            user_nodes.c.key
        ).cte('organization_roles')

        select_account_roles = select([
            user_nodes.c.key,
            func.json_agg(
                func.json_build_object(
                    'name', accounts.c.name,
                    'scope_key', accounts.c.key,
                    'role', account_members.c.role
                )
            ).label('account_roles')
        ]).select_from(
            user_nodes.join(
                account_members, user_nodes.c.key == account_members.c.user_key
            ).join(
                accounts, account_members.c.account_id == accounts.c.id
            )
        ).group_by(
            user_nodes.c.key
        ).cte('account_roles')

        select_stmt = select([
            user_nodes.c.key,
            select_account_roles.c.account_roles,
            select_organization_roles.c.organization_roles,
            select_system_roles.c.system_roles
        ]).select_from(
            user_nodes.outerjoin(
                select_organization_roles, select_organization_roles.c.key == user_nodes.c.key
            ).outerjoin(
                select_account_roles, select_account_roles.c.key == user_nodes.c.key
            ).outerjoin(
                select_system_roles, select_system_roles.c.key == user_nodes.c.key
            )
        )
        return select_stmt
