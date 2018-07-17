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
from .organization import Organization
from .project import Project

class Query(graphene.ObjectType):
    node = relay.Node.Field()
    account = graphene.Field(Account)
    organization = graphene.Field(
        Organization,
        organization_key = graphene.Argument(type=graphene.String, required=True)
    )
    project = graphene.Field(
        Project,
        project_key = graphene.Argument(type=graphene.String, required=True)
    )

    def resolve_account(self, info, **kwargs):
        return Account.resolve_field(self, info, **kwargs)

    def resolve_organization(self, info, organization_key, **kwargs):
        return Organization.resolve_field(self, info, organization_key,  **kwargs)

    def resolve_project(self, info, project_key, **kwargs):
        return Organization.resolve_field(self, info, project_key,  **kwargs)

schema = graphene.Schema(query=Query)
