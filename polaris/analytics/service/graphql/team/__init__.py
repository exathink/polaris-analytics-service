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
from .selectable import TeamNode


class Team(
    NamedNodeResolverMixin,
    Selectable
):
    class Meta:
        interfaces = (NamedNode, )
        named_node_resolver = TeamNode

        connection_class = lambda: Teams

    @classmethod
    def Field(cls, **kwargs):
        return super().Field(**kwargs)

    @classmethod
    def resolve_field(cls, parent, info, team_key, **kwargs):
        return cls.resolve_instance(key=team_key, **kwargs)


class Teams(
    CountableConnection
):
    class Meta:
        node = Team


class TeamsConnectionMixin(ConnectionResolverMixin):
    users = Team.ConnectionField()

    def resolve_teams(self, info, **kwargs):
        return Team.resolve_connection(
            self.get_connection_resolver_context('teams'),
            self.get_connection_node_resolver('teams'),
            self.get_instance_query_params(),
            **kwargs
        )
