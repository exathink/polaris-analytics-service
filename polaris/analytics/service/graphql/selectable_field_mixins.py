# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.selectable import SimpleSelectableResolverMixin
from .selectable_fields import CumulativeCommitCountField, WeeklyContributorCountsField, CycleMetricsTrendsField
from .interface_mixins import KeyIdResolverMixin


class SelectablePropertyResolverMixin(KeyIdResolverMixin, SimpleSelectableResolverMixin):

    def resolve_selectable_field(self, property_name, **kwargs):
        selectable = self._meta.selectable_field_resolvers.get(property_name)
        return self.resolve_selectable(selectable, self.get_instance_query_params(), **kwargs)


class CumulativeCommitCountResolverMixin(SelectablePropertyResolverMixin):

    cumulative_commit_count = graphene.Field(graphene.List(CumulativeCommitCountField))

    def resolve_cumulative_commit_count(self, info, **kwargs):
        return self.resolve_selectable_field('cumulative_commit_count', **kwargs)


class WeeklyContributorCountsResolverMixin(SelectablePropertyResolverMixin):

    weekly_contributor_counts = graphene.Field(graphene.List(WeeklyContributorCountsField))

    def resolve_weekly_contributor_counts(self, info, **kwargs):
        return self.resolve_selectable_field('weekly_contributor_counts', **kwargs)


class CycleMetricsTrendsResolverMixin(SelectablePropertyResolverMixin):

    cycle_metrics_trends = graphene.Field(
        graphene.List(CycleMetricsTrendsField),
        before=graphene.Argument(
            graphene.DateTime,
            required=False,
            description="Specifies an end date for the measurement period. Default is current UTC timestamp "
                        "when the request is processed on the server."
        ),
        days=graphene.Argument(
            graphene.Int,
            required=True,
            description="specifies the days over which the trends should be evaluated. The measurement period for"
                        "the trends is the time interval [before-days, before]."
                        "eg. if days=30 then we will show the trends for each selected metric for the 30 day period "
                        "ending with the 'before' date",

        ),
        measurement_window=graphene.Argument(
            graphene.Int,
            required=True,
            description="Specifies the window of days over which the trend metrics are aggregated: "
                        "eg. if days=30 and trend_window=15 and the metric being reported is avg_lead_time"
                        "average lead time will be computed by taking the average over the prior 15 days of data for "
                        "each valid date in the 30 day measurement period"
        ),

    )

    def resolve_cycle_metrics_trends(self, info, **kwargs):
        return self.resolve_selectable_field('cycle_metrics_trends', **kwargs)


