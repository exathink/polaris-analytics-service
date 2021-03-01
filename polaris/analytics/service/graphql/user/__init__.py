# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.selectable import Selectable, CountableConnection, ConnectionResolverMixin
from polaris.graphql.interfaces import NamedNode
from ..interfaces import UserInfo, ScopedRole
from ..interface_mixins import NamedNodeResolverMixin
from .selectable import UserNode, UserUserInfo


class User(
    NamedNodeResolverMixin,
    Selectable
):
    class Meta:
        interfaces = (NamedNode, UserInfo, ScopedRole)
        named_node_resolver = UserNode
        interface_resolvers = {
            'UserInfo': UserUserInfo
        }
        connection_class = lambda: Users

    @classmethod
    def Field(cls, **kwargs):
        return super().Field(key_is_required=False, **kwargs)

    @classmethod
    def resolve_field(cls, info, user_key, **kwargs):
        return cls.resolve_instance(key=user_key, **kwargs)


class Users(
    CountableConnection
):
    class Meta:
        node = User


class UsersConnectionMixin(ConnectionResolverMixin):
    users = User.ConnectionField(
        active_only=graphene.Argument(
            graphene.Boolean,
            required=False,
            description="When getting user info "
                        "include only active users",
            default_value=True
        ),
    )

    def resolve_users(self, info, **kwargs):
        return User.resolve_connection(
            self.get_connection_resolver_context('users'),
            self.get_connection_node_resolver('users'),
            self.get_instance_query_params(),
            join_field='key',
            **kwargs
        )
