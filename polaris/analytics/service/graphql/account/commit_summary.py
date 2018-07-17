# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
import datetime
from sqlalchemy import text
from polaris.common import db

from .commit_summary_for_account import CommitSummaryForAccount
from .commit_summary_by_organization import AccountCommitSummaryByOrganization
from .commit_summary_by_project import AccountCommitSummaryByProject
from .commit_summary_by_repository import AccountCommitSummaryByRepository
from .enums import AccountPartitions

class AccountCommitSummary(graphene.Union):

    class Meta:
        types = (
            CommitSummaryForAccount,
            AccountCommitSummaryByOrganization,
            AccountCommitSummaryByProject,
            AccountCommitSummaryByRepository
        )
    @classmethod
    def Field(cls):
        return graphene.Field(
        graphene.List(AccountCommitSummary),
        group_by=graphene.Argument(type=AccountPartitions, required=False, default_value=AccountPartitions.account)
    )

    @classmethod
    def resolve_type(cls, instance, info):
        if instance.type == AccountPartitions.account.value:
            return CommitSummaryForAccount
        elif instance.type == AccountPartitions.organization.value:
            return AccountCommitSummaryByOrganization
        elif instance.type == AccountPartitions.project.value:
            return AccountCommitSummaryByProject
        elif instance.type == AccountPartitions.repository.value:
            return AccountCommitSummaryByRepository


    @classmethod
    def resolve(cls, account, info, **kwargs):
        query = None
        if kwargs.get('group_by') == AccountPartitions.account:
            return CommitSummaryForAccount.resolve(account, info, **kwargs)
        elif kwargs.get('group_by') == AccountPartitions.organization:
            return AccountCommitSummaryByOrganization.resolve(account, info, **kwargs)
        elif kwargs.get('group_by') == AccountPartitions.project:
            return AccountCommitSummaryByProject.resolve(account, info, **kwargs)
        elif kwargs.get('group_by') == AccountPartitions.repository:
            return AccountCommitSummaryByRepository.resolve(account, info, **kwargs)










