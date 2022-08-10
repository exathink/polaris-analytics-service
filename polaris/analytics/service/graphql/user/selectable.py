# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import graphene
from sqlalchemy import select, bindparam, func
from polaris.auth.db.model import users, roles, users_roles
from polaris.analytics.db.model import organizations, organization_members, accounts, account_members, \
    accounts_organizations
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
    def get_system_roles_selector(user_nodes):
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
        return select_system_roles

    @staticmethod
    def get_organization_roles_selector(user_nodes, user_roles_args):
        user_organizations_rel = user_nodes.join(
            organization_members, user_nodes.c.key == organization_members.c.user_key
        ).join(
            organizations, organization_members.c.organization_id == organizations.c.id
        )
        if user_roles_args.get('account_key'):
            user_organizations_rel = user_organizations_rel.join(
                accounts_organizations, accounts_organizations.c.organization_id == organizations.c.id
            ).join(
                accounts, accounts_organizations.c.account_id == accounts.c.id
            )
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
            user_organizations_rel
        )
        if user_roles_args.get('account_key'):
            select_organization_roles = select_organization_roles.where(
                accounts.c.key == user_roles_args.get('account_key')
            )
        select_organization_roles = select_organization_roles.group_by(
            user_nodes.c.key
        ).cte('organization_roles')
        return select_organization_roles

    @staticmethod
    def get_account_roles_selector(user_nodes, user_roles_args):
        select_account_roles = select([
            user_nodes.c.key,
            accounts.c.name,
            accounts.c.key.label('account_key'),
            account_members.c.role
        ]).select_from(
            user_nodes.join(
                account_members, user_nodes.c.key == account_members.c.user_key
            ).join(
                accounts, account_members.c.account_id == accounts.c.id
            )
        )
        if user_roles_args.get('account_key'):
            select_account_roles = select_account_roles.where(
                accounts.c.key == user_roles_args.get('account_key')
            )

        select_account_roles = select_account_roles.alias('select_account_roles')

        return select([
            select_account_roles.c.key,
            func.json_agg(
                func.json_build_object(
                    'name', select_account_roles.c.name,
                    'scope_key', select_account_roles.c.account_key,
                    'role', select_account_roles.c.role
                )
            ).label('account_roles')
        ]).select_from(
            select_account_roles
        ).group_by(
            select_account_roles.c.key
        ).cte('account_roles')

    @staticmethod
    def selectable(user_nodes, **kwargs):

        user_roles_args = kwargs.get('user_roles_args', {})

        select_system_roles = UserUserRoles.get_system_roles_selector(user_nodes)

        select_organization_roles = UserUserRoles.get_organization_roles_selector(user_nodes, user_roles_args)

        select_account_roles = UserUserRoles.get_account_roles_selector(user_nodes, user_roles_args)

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
