# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene


from flask_security import current_user

from ..interfaces import CommitSummary, NamedNode
from polaris.analytics.service.graphql.mixins import \
    NamedNodeResolverMixin, \
    KeyIdResolverMixin, \
    CommitSummaryResolverMixin

from .account_commit_summary import AccountCommitSummary
from .account_organizations import AccountOrganizations
from .account_repositories import AccountRepositories
from .account_projects import AccountProjects

from polaris.common import db

from polaris.repos.db.model import Account as AccountModel


class Account(
    KeyIdResolverMixin,
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    graphene.ObjectType
):
    class Meta:
        interfaces = (NamedNode, CommitSummary)

    organizations = graphene.Field(AccountOrganizations)
    projects = graphene.Field(AccountProjects)
    repositories = graphene.Field(AccountRepositories)

    @classmethod
    def resolve_field(cls, key, info, **kwargs):
        if key == str(current_user.account_key):
            return Account(key=current_user.account_key)

    @staticmethod
    def load_instance(key, info, **kwargs):
        with db.orm_session() as session:
            return AccountModel.find_by_account_key(session, key)


    def resolve_organizations(self, info, **kwargs):
        return AccountOrganizations.resolve(self, info, **kwargs)

    def resolve_projects(self, info, **kwargs):
        return AccountProjects.resolve(self, info, **kwargs)

    def resolve_repositories(self, info, **kwargs):
        return AccountRepositories.resolve(self, info, **kwargs)


    def resolve_commit_summary(self, info, **kwargs):
            return AccountCommitSummary.resolve(self.key, info, **kwargs)



