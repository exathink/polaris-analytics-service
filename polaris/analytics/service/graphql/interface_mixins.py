# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.graphql.mixins import *

from polaris.graphql.utils import init_tuple, create_tuple

from .interfaces import \
    CommitInfo, \
    CommitSummary, \
    ContributorCount, \
    ProjectCount, \
    RepositoryCount, \
    OrganizationRef, \
    CommitChangeStats


class CommitSummaryResolverMixin(KeyIdResolverMixin):
    commit_summary_tuple = create_tuple(CommitSummary)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commit_summary = init_tuple(self.commit_summary_tuple, **kwargs)

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
        return self.get_commit_summary(info, **kwargs).earliest_commit

    def resolve_latest_commit(self, info, **kwargs):
        return self.get_commit_summary(info, **kwargs).latest_commit

    def resolve_commit_count(self, info, **kwargs):
        return self.get_commit_summary(info, **kwargs).commit_count


class CommitInfoResolverMixin(KeyIdResolverMixin):
    commit_tuple = create_tuple(CommitInfo)
    stats_tuple = create_tuple(CommitChangeStats)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.commit_info = init_tuple(self.commit_tuple, **kwargs)

    def resolve_commit(self, info, **kwargs):
        return self.resolve_interface_for_instance(
            interface=['Commit'],
            params=self.get_instance_query_params(),
            **kwargs
        )

    def get_commit(self, info, **kwargs):
        if self.commit_info is None:
            self.commit_info = self.resolve_commit(info, **kwargs)
        return self.commit_info

    def resolve_commit_hash(self, info, **kwargs):
        return self.get_commit(info, **kwargs).commit_hash

    def resolve_repository(self, info, **kwargs):
        return self.get_commit(info, **kwargs).repository

    def resolve_repository_key(self, info, **kwargs):
        return self.get_commit(info, **kwargs).repository_key

    def resolve_repository_url(self, info, **kwargs):
        return self.get_commit(info, **kwargs).repository_url

    def resolve_commit_date(self, info, **kwargs):
        return self.get_commit(info, **kwargs).commit_date

    def resolve_committer(self, info, **kwargs):
        return self.get_commit(info, **kwargs).committer

    def resolve_committer_key(self, info, **kwargs):
        return self.get_commit(info, **kwargs).committer_key

    def resolve_author_date(self, info, **kwargs):
        return self.get_commit(info, **kwargs).author_date

    def resolve_author(self, info, **kwargs):
        return self.get_commit(info, **kwargs).author

    def resolve_author_key(self, info, **kwargs):
        return self.get_commit(info, **kwargs).author_key

    def resolve_commit_message(self, info, **kwargs):
        return self.get_commit(info, **kwargs).commit_message

    def resolve_num_parents(self, info, **kwargs):
        return self.get_commit(info, **kwargs).num_parents

    def resolve_stats(self, info, **kwargs):
        return init_tuple(self.stats_tuple, **self.get_commit(info, **kwargs).stats)


class ContributorCountResolverMixin(KeyIdResolverMixin):
    contributor_count_tuple_type = create_tuple(ContributorCount)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.contributor_count = init_tuple(self.contributor_count_tuple_type, **kwargs)

    def resolve_contributor_count_interface(self, info, **kwargs):
        return self.resolve_interface_for_instance(
            interface=['ContributorCount'],
            params=self.get_instance_query_params(),
            **kwargs
        )

    def get_contributor_count(self, info, **kwargs):
        if self.contributor_count is None:
            self.contributor_count = self.resolve_contributor_count_interface(info, **kwargs)

        return self.contributor_count

    def resolve_contributor_count(self, info, **kwargs):
        return self.get_contributor_count(info, **kwargs).contributor_count


class ProjectCountResolverMixin(KeyIdResolverMixin):
    project_count_tuple_type = create_tuple(ProjectCount)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_count = init_tuple(self.project_count_tuple_type, **kwargs)

    def resolve_interface_project_count(self, info, **kwargs):
        return self.resolve_interface_for_instance(
            interface=['ProjectCount'],
            params=self.get_instance_query_params(),
            **kwargs
        )

    def get_project_count(self, info, **kwargs):
        if self.project_count is None:
            self.project_count = self.resolve_interface_project_count(info, **kwargs)

        return self.project_count

    def resolve_project_count(self, info, **kwargs):
        return self.get_project_count(info, **kwargs).project_count


class RepositoryCountResolverMixin(KeyIdResolverMixin):
    repository_count_tuple_type = create_tuple(RepositoryCount)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.repository_count = init_tuple(self.repository_count_tuple_type, **kwargs)

    def resolve_interface_repository_count(self, info, **kwargs):
        return self.resolve_interface_for_instance(
            interface=['RepositoryCount'],
            params=self.get_instance_query_params(),
            **kwargs
        )

    def get_repository_count(self, info, **kwargs):
        if self.repository_count is None:
            self.repository_count = self.resolve_interface_repository_count(info, **kwargs)

        return self.repository_count

    def resolve_repository_count(self, info, **kwargs):
        return self.get_repository_count(info, **kwargs).repository_count


class OrganizationRefResolverMixin(KeyIdResolverMixin):
    organization_ref_tuple_type = create_tuple(OrganizationRef)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.organization_ref = init_tuple(self.organization_ref_tuple_type, **kwargs)

    def resolve_organization_ref(self, info, **kwargs):
        return self.resolve_interface_for_instance(
            interface=['OrganizationRef'],
            params=self.get_instance_query_params(),
            **kwargs
        )

    def get_organization_ref(self, info, **kwargs):
        if self.organization_ref is None:
            self.organization_ref = self.resolve_organization_ref(info, **kwargs)

        return self.organization_ref

    def resolve_organization_name(self, info, **kwargs):
        return self.get_organization_ref(info, **kwargs).organization_name

    def resolve_organization_key(self, info, **kwargs):
        return self.get_organization_ref(info, **kwargs).organization_key
