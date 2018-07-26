# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene
from ..organization import Organization
from .account_organizations_commit_summaries import AccountOrganizationsCommitSummaries
from .account_organizations_count import AccountOrganizationsCount

from ..mixins import KeyIdResolverMixin

class AccountOrganizations(
    KeyIdResolverMixin,
    graphene.ObjectType
):

    class Meta:
        interfaces = (graphene.relay.Node, )

    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key

    count = graphene.Field(graphene.Int)
    commit_summaries = graphene.Field(graphene.List(Organization))



    @classmethod
    def Field(cls):
        return graphene.Field(AccountOrganizations)

    @classmethod
    def resolve(cls, account, info, **kwargs):
        return AccountOrganizations(key=account.key)

    def resolve_commit_summaries(self, info, **kwargs):
        return AccountOrganizationsCommitSummaries.resolve(self.key, info, **kwargs)

    def resolve_count(self, info, **kwargs):
        return AccountOrganizationsCount.resolve(self.key, info, **kwargs)
