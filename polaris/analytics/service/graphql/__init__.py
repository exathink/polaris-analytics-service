# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
__version__ = '0.0.1'

import graphene

from polaris.graphql.interfaces import NamedNode
from .account import Account
from .organization import Organization
from .project import Project
from .repository import Repository


class Query(graphene.ObjectType):
    node = NamedNode.Field()

    account = Account.Field()
    organization = Organization.Field()
    project = Project.Field()
    repository = Repository.Field()


    def resolve_account(self, info, key,  **kwargs):
        return Account.resolve_field(info, key, **kwargs)

    def resolve_organization(self, info, key, **kwargs):
        return Organization.resolve_field(self, info, key,  **kwargs)

    def resolve_project(self, info, key, **kwargs):
        return Project.resolve_field(self, info, key,  **kwargs)

    def resolve_repository(self, info, key, **kwargs):
        return Repository.resolve_field(self,info, key, **kwargs)


schema = graphene.Schema(query=Query)
