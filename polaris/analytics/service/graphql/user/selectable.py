# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar



import graphene
from sqlalchemy import select, bindparam, func
from polaris.auth.db.model import users
from polaris.analytics.db.model import organizations, organization_members
from polaris.graphql.interfaces import KeyIdNode, NamedNode
from ..interfaces import UserInfo


class UserNode:
    interfaces = (NamedNode, )

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
            users.c.email,
            func.json_agg(
                func.json_build_object(
                    'organization_key', organizations.c.key,
                    'organization_name', organizations.c.name,
                    'organization_role', organization_members.c.role,
                    'organization_name', organizations.c.name
                )
            ).label('organization_roles')

        ]).select_from(
            user_node.outerjoin(
                users, user_node.c.key == users.c.key
            ).join(
                organization_members, organization_members.c.user_key == users.c.key
            ).join(
                organizations, organization_members.c.organization_id == organizations.c.id
            )
        ).group_by(
            users.c.key,
            users.c.first_name,
            users.c.last_name,
            users.c.email
        )
        if kwargs.get('active_only'):
            return select_stmt.where(
                users.c.active == True
            )
        else:
            return select_stmt