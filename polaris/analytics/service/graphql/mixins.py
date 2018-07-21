# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from polaris.analytics.service.graphql.interfaces import CommitSummary


class NamedNodeResolverMixin:
    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        self.name = kwargs.get('name')
        self.instance = None

    @staticmethod
    def load_instance(key, info, **kwargs):
       return NotImplemented()

    def get_instance(self, info, **kwargs):
        if self.instance is None:
            self.instance = self.load_instance(self.key, info, **kwargs)
        return self.instance

    def resolve_key(self, info, **kwargs):
        return self.key

    def resolve_name(self, info, **kwargs):
        if self.name is None:
            self.name = self.get_instance(info, **kwargs).name

        return self.name


class KeyIdResolverMixin:
    def resolve_id(self, info, **kwargs):
        return self.key


class CommitSummaryResolverMixin:
    def resolve_commit_summary(self, info, **kwargs):
       return NotImplemented()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commit_summary = None

    def get_commit_summary(self, info, **kwargs):
        if self.commit_summary is None:
            self.commit_summary = self.resolve_commit_summary(info, **kwargs) or CommitSummary.UnResolved()

        return self.commit_summary

    def resolve_earliest_commit(self, info, **kwargs):
        return self.get_commit_summary(info,**kwargs).earliest_commit

    def resolve_latest_commit(self, info, **kwargs):
        return self.get_commit_summary(info, **kwargs).latest_commit

    def resolve_contributor_count(self, info, **kwargs):
        return self.get_commit_summary(info, **kwargs).contributor_count

    def resolve_commit_count(self, info, **kwargs):
        return self.get_commit_summary(info, **kwargs).commit_count