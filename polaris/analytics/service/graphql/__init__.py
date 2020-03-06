# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
__version__ = '0.0.1'

import graphene

from polaris.graphql.interfaces import NamedNode
from .viewer import Viewer
from .viewer.mutations import ViewerMutationsMixin

from .account import Account
from .account.mutations import AccountMutationsMixin

from .user.mutations import UseMutationsMixin

from .commit import Commit
from .organization import Organization
from .project import Project
from .project.mutations import ProjectMutationsMixin
from .feature_flag.mutations import FeatureFlagMutationsMixin

from .repository import Repository
from .contributor import Contributor, ContributorMutationsMixin
from .public import Public
from .work_item import WorkItem
from .work_items_source import WorkItemsSource
from .feature_flag import FeatureFlag
from .summarizers import *


class Query(graphene.ObjectType):
    node = NamedNode.Field()

    viewer = Viewer.Field()
    account = Account.Field()
    commit = Commit.Field()
    organization = Organization.Field()
    project = Project.Field()
    repository = Repository.Field()
    contributor = Contributor.Field()
    public = Public.Field()
    work_item = WorkItem.Field()
    work_items_source = WorkItemsSource.Field()
    feature_flag = FeatureFlag.Field()

    all_accounts = Account.ConnectionField()
    all_feature_flags = FeatureFlag.ConnectionField(active_only=graphene.Boolean(required=False))

    def resolve_viewer(self, info, **kwargs):
        return Viewer.resolve_field(info, **kwargs)

    def resolve_account(self, info,  **kwargs):
        return Account.resolve_field(info, **kwargs)

    def resolve_commit(self, info, key, **kwargs):
        return Commit.resolve_field(info, key, **kwargs)

    def resolve_organization(self, info, key, **kwargs):
        return Organization.resolve_field(self, info, key,  **kwargs)

    def resolve_project(self, info, key, **kwargs):
        return Project.resolve_field(self, info, key,  **kwargs)

    def resolve_repository(self, info, key, **kwargs):
        return Repository.resolve_field(self,info, key, **kwargs)

    def resolve_contributor(self, info, key, **kwargs):
        return Contributor.resolve_field(self,info, key, **kwargs)

    def resolve_work_item(self, info, key, **kwargs):
        return WorkItem.resolve_field(self, info, key, **kwargs)

    def resolve_work_items_source(self, info, key, **kwargs):
        return WorkItemsSource.resolve_field(self, info, key, **kwargs)

    def resolve_feature_flag(self, info, **kwargs):
        return FeatureFlag.resolve_field(self, info, **kwargs)

    def resolve_public(self, info, **kwargs):
        return Public.resolve_field(info, **kwargs)

    def resolve_all_accounts(self, info, **kwargs):
        return Account.resolve_all_accounts(info, **kwargs)

    def resolve_all_feature_flags(self, info, **kwargs):
        return FeatureFlag.resolve_all_feature_flags(info, **kwargs)

class Mutations(
    graphene.ObjectType,
    AccountMutationsMixin,
    ContributorMutationsMixin,
    ViewerMutationsMixin,
    UseMutationsMixin,
    ProjectMutationsMixin,
    FeatureFlagMutationsMixin
):
    pass


schema = graphene.Schema(query=Query, mutation=Mutations)
