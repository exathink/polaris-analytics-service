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
from .repository import Repository

class Query(graphene.ObjectType):
    node = relay.Node.Field()
    account = graphene.Field(
        Account,
        key = graphene.Argument(type=graphene.String, required=False)
    )
    organization = graphene.Field(
        Organization,
        key = graphene.Argument(type=graphene.String, required=True)
    )
    project = graphene.Field(
        Project,
        key = graphene.Argument(type=graphene.String, required=True)
    )

    repository = graphene.Field(
        Repository,
        key = graphene.Argument(type=graphene.String, required=True)
    )

    def resolve_account(self, info, **kwargs):
        return Account.resolve_field(info, **kwargs)

    def resolve_organization(self, info, key, **kwargs):
        return Organization.resolve_field(self, info, key,  **kwargs)

    def resolve_project(self, info, key, **kwargs):
        return Project.resolve_field(self, info, key,  **kwargs)

    def resolve_repository(self, info, key, **kwargs):
        return Repository.resolve_field(self,info, key, **kwargs)


schema = graphene.Schema(query=Query)
