# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene

from ..organization import Organization
from .commit_summary_by_organization import AccountCommitSummaryByOrganization

class AccountOrganizations(graphene.ObjectType):

    def __init__(self, account, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.account = account

    commit_summaries = graphene.Field(graphene.List(Organization))



    @classmethod
    def Field(cls):
        return graphene.Field(AccountOrganizations)

    @classmethod
    def resolve(cls, account, info, **kwargs):
        return AccountOrganizations(account)

    def resolve_commit_summaries(self, info, **kwargs):
        return AccountCommitSummaryByOrganization.resolve(self.account.key, info, **kwargs)
