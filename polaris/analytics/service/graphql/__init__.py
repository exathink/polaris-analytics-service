# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
__version__ = '0.0.1'

import graphene
from graphene import relay

from .account import Account

class Query(graphene.ObjectType):
    node = relay.Node.Field()
    account = graphene.Field(Account)

    def resolve_account(self, info, **kwargs):
        return Account.resolve_field(self, info, **kwargs)



schema = graphene.Schema(query=Query)
