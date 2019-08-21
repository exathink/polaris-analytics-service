# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
from polaris.analytics.db import api, aggregations

from test.fixtures.repository_commit_scope_resolution import *


class TestGithubWorkItemSourceRepositoryResolution:

    def it_resolves_when_work_items_sources_are_created(self, setup_work_items_source_repo_resolution):
        organization, repository = setup_work_items_source_repo_resolution

        work_items_source_key = str(uuid.uuid4())
        work_items_source_summary = dict(
            key=work_items_source_key,
            name='foo',
            integration_type='github',
            work_items_source_type='repository_issues',
            commit_mapping_scope='repository',
            source_id=repository.source_id
        )
        result = api.register_work_items_source(
            organization.key,
            work_items_source_summary
        )

        assert result['success']
        assert db.connection().execute(
            f"select commit_mapping_scope_key from analytics.work_items_sources where key='{work_items_source_key}'"
        ).scalar() == repository.key

    def it_resolves_repositories_are_created(self, setup_repo_work_items_source_resolution):
        organization, work_items_source = setup_repo_work_items_source_resolution
        repository_key = uuid.uuid4()

        repository_summaries = [
            dict(
                name='rails',
                key=repository_key,
                source_id=work_items_source.source_id
            )
        ]

        result = aggregations.resolve_work_items_sources_for_repositories(
            organization.key,
            repository_summaries
        )

        assert db.connection().execute(
            f"select commit_mapping_scope_key from analytics.work_items_sources where key='{work_items_source.key}'"
        ).scalar() == repository_key
