# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from polaris.analytics.service.graphql.interfaces import *


class NamedNodeResolverMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = kwargs.get('name', None)
        self.instance = None

    @staticmethod
    def load_instance(key, info, **kwargs):
        return NotImplemented()

    def get_instance(self, info, **kwargs):
        if self.instance is None:
            self.instance = self.load_instance(self.key, info, **kwargs)
        return self.instance

    def resolve_name(self, info, **kwargs):
        if self.name is None:
            self.name = self.get_instance(info, **kwargs).name

        return self.name


class KeyIdResolverMixin:
    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key

    def resolve_id(self, info, **kwargs):
        return self.key


class CommitSummaryResolverMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.earliest_commit = kwargs.get('earliest_commit')
        self.latest_commit = kwargs.get('latest_commit')
        self.commit_count = kwargs.get('commit_count')
        self.commit_summary = None

    def resolve_commit_summary(self, info, **kwargs):
        return CommitSummary.UnResolved()

    def get_commit_summary(self, info, **kwargs):
        if self.commit_summary is None:
            self.commit_summary = self.resolve_commit_summary(info, **kwargs) or CommitSummary.UnResolved()
        return self.commit_summary

    def resolve_earliest_commit(self, info, **kwargs):
        return self.earliest_commit or self.get_commit_summary(info,**kwargs).earliest_commit

    def resolve_latest_commit(self, info, **kwargs):
        return self.latest_commit or self.get_commit_summary(info, **kwargs).latest_commit

    def resolve_commit_count(self, info, **kwargs):
        return self.commit_count or self.get_commit_summary(info, **kwargs).commit_count


class ContributorSummaryResolverMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.contributor_count = kwargs.get('contributor_count')
        self.contributor_summary = None

    def resolve_contributor_summary(self, info, **kwargs):
        return ContributorSummary.UnResolved()

    def get_contributor_summary(self, info, **kwargs):
        if self.contributor_summary is None:
            self.contributor_summary = self.resolve_contributor_summary(info, **kwargs) or \
                                       ContributorSummary.UnResolved()
        return self.contributor_summary


    def resolve_contributor_count(self, info, **kwargs):
        return self.contributor_count or \
               self.get_contributor_summary(info, **kwargs).contributor_count
