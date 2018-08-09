# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.graphql.mixins import *
from collections import namedtuple

from polaris.graphql.utils import init_tuple, create_tuple

from .interfaces import CommitSummary, ContributorSummary

class CommitSummaryResolverMixin(KeyIdResolverMixin):
    tuple_type = create_tuple(CommitSummary)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commit_summary = init_tuple(self.tuple_type, **kwargs)


    def resolve_commit_summary(self, info, **kwargs):
        return self.resolve_interface_for_instance(
            interface=['CommitSummary'],
            params=self.get_instance_query_params(),
            **kwargs
        )


    def get_commit_summary(self, info, **kwargs):
        if self.commit_summary is None:
            self.commit_summary = self.resolve_commit_summary(info, **kwargs)
        return self.commit_summary

    def resolve_earliest_commit(self, info, **kwargs):
        return self.get_commit_summary(info,**kwargs).earliest_commit

    def resolve_latest_commit(self, info, **kwargs):
        return self.get_commit_summary(info, **kwargs).latest_commit

    def resolve_commit_count(self, info, **kwargs):
        return self.get_commit_summary(info, **kwargs).commit_count


class ContributorSummaryResolverMixin(KeyIdResolverMixin):
    tuple_type = create_tuple(ContributorSummary)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.contributor_summary = None

    def resolve_contributor_summary(self, info, **kwargs):
        return self.resolve_interface_for_instance(
            interface=['ContributorSummary'],
            params=self.get_instance_query_params(),
            **kwargs
        )


    def get_contributor_summary(self, info, **kwargs):
        if self.contributor_summary is None:
            self.contributor_summary = self.resolve_contributor_summary(info, **kwargs)

        return self.contributor_summary


    def resolve_contributor_count(self, info, **kwargs):
        return self.get_contributor_summary(info, **kwargs).contributor_count

    def resolve_unassigned_alias_count(self, info, **kwargs):
        return self.get_contributor_summary(info, **kwargs).unassigned_alias_count

    def resolve_unique_contributor_count(self, info, **kwargs):
        return self.get_contributor_summary(info, **kwargs).unique_contributor_count

