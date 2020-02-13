# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.selectable import SimpleSelectableResolverMixin
from .selectable_fields import CumulativeCommitCountField, WeeklyContributorCountsField, \
    DistinctStatesField
from .interface_mixins import KeyIdResolverMixin


class SelectablePropertyResolverMixin(KeyIdResolverMixin, SimpleSelectableResolverMixin):

    def resolve_selectable_field(self, property_name, **kwargs):
        selectable = self._meta.selectable_field_resolvers.get(property_name)
        return self.resolve_selectable(selectable, self.get_instance_query_params(), **kwargs)


class CumulativeCommitCountResolverMixin(SelectablePropertyResolverMixin):

    cumulative_commit_count = graphene.Field(graphene.List(CumulativeCommitCountField))

    def resolve_cumulative_commit_count(self, info, **kwargs):
        return self.resolve_selectable_field('cumulative_commit_count')


class WeeklyContributorCountsResolverMixin(SelectablePropertyResolverMixin):

    weekly_contributor_counts = graphene.Field(graphene.List(WeeklyContributorCountsField))

    def resolve_weekly_contributor_counts(self, info, **kwargs):
        return self.resolve_selectable_field('weekly_contributor_counts')


class DistinctStatesResolverMixin(SelectablePropertyResolverMixin):

    distinct_states = graphene.Field(graphene.List(DistinctStatesField))

    def resolve_distinct_states(self, info, **kwargs):
        return self.resolve_selectable_field('distinct_states')