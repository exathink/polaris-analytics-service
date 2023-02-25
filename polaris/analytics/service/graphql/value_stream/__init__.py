# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable, ConnectionResolverMixin

from ..interfaces import ValueStreamInfo
from ..interface_mixins import KeyIdResolverMixin, NamedNodeResolverMixin
from polaris.graphql.connection_utils import CountableConnection

from .selectable import ValueStreamNode

class ValueStream(
    # interface mixins
    NamedNodeResolverMixin,
    # interface mixins

    Selectable
):
    class Meta:
        description = """
Project: A NamedNode representing a value stream. 
            

"""
        interfaces = (
            # ----Implicit Interfaces ------- #
            NamedNode,
            ValueStreamInfo

        )
        named_node_resolver = ValueStreamNode
        interface_resolvers = {
        }
        connection_node_resolvers = {
        }
        selectable_field_resolvers = {
        }
        connection_class = lambda: ValueStreams

    @classmethod
    def Field(cls, key_is_required=True, **kwargs):
        return super().Field(
            key_is_required,
            **kwargs
        )

    @classmethod
    def resolve_field(cls, parent, info, value_stream_key, **kwargs):
        return cls.resolve_instance(key=value_stream_key, **kwargs)


class ValueStreams(
    CountableConnection
):
    class Meta:
        node = ValueStream



class ValueStreamsConnectionMixin(KeyIdResolverMixin, ConnectionResolverMixin):
    value_streams = ValueStream.ConnectionField()

    def resolve_value_streams(self, info, **kwargs):
        return ValueStream.resolve_connection(
            self.get_connection_resolver_context('value_streams'),
            self.get_connection_node_resolver('value_streams'),
            self.get_instance_query_params(),
            **kwargs
        )



