# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from .commit_summary_by_organization import AccountCommitSummaryByOrganization
from .commit_summary_by_project import AccountCommitSummaryByProject
from .commit_summary_by_repository import AccountCommitSummaryByRepository
from .commit_summary_for_account import CommitSummaryForAccount


class AccountCommitSummary(graphene.ObjectType):
    account_key = graphene.Field(graphene.UUID)
    for_account = graphene.Field(CommitSummaryForAccount)
    by_organization = graphene.Field(graphene.List(AccountCommitSummaryByOrganization))
    by_project = graphene.Field(graphene.List(AccountCommitSummaryByProject))
    by_repository = graphene.Field(graphene.List(AccountCommitSummaryByRepository))

    @classmethod
    def Field(cls):
        return graphene.Field(AccountCommitSummary)

    @classmethod
    def resolve(cls, account, info, **kwargs):
        return AccountCommitSummary(account_key=account.id)

    def resolve_for_account(self, info, **kwargs):
        return CommitSummaryForAccount.resolve(self.account_key, info, **kwargs)

    def resolve_by_organization(self, info, **kwargs):
        return AccountCommitSummaryByOrganization.resolve(self.account_key, info, **kwargs)

    def resolve_by_project(self, info, **kwargs):
        return AccountCommitSummaryByProject.resolve(self.account_key, info, **kwargs)

    def resolve_by_repository(self, info, **kwargs):
        return AccountCommitSummaryByRepository.resolve(self.account_key, info, **kwargs)
