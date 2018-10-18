# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene
from ..selectable_field_mixins import SelectablePropertyResolverMixin
from ..interfaces import NamedNode, CommitSummary, CommitCount


class ContributorRepositoriesActivitySummaryField(graphene.ObjectType):
    class Meta:
        interfaces = (NamedNode, CommitSummary)


class ContributorRepositoriesActivitySummaryResolverMixin(SelectablePropertyResolverMixin):

    repositories_activity_summary = graphene.Field(graphene.List(ContributorRepositoriesActivitySummaryField))

    def resolve_repositories_activity_summary(self, info, **kwargs):
        return self.resolve_selectable_field('repositories_activity_summary')


class ContributorRecentlyActiveRepositoriesField(graphene.ObjectType):
    class Meta:
        interfaces = (NamedNode, CommitCount)


class ContributorRecentlyActiveRepositoriesResolverMixin(SelectablePropertyResolverMixin):
    recently_active_repositories = graphene.Field(
        graphene.List(ContributorRecentlyActiveRepositoriesField),
        top=graphene.Argument(graphene.Int, required=False),
        before=graphene.Argument(graphene.DateTime, required=False),
        days=graphene.Argument(graphene.Int, required=False, default_value=7)
    )

    def resolve_recently_active_repositories(self, info, **kwargs):
        return self.resolve_selectable_field('recently_active_repositories', **kwargs)
