# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.fixtures.repo_org import *

import uuid

from polaris.analytics.db import api
from polaris.common.enums import VcsIntegrationTypes


class TestImportRepositories:

    def it_imports_new_repositories(self, setup_org):
        organization = setup_org
        repositories = [
            dict(
                name='A new repo',
                key=uuid.uuid4(),
                description='A new new repo',
                url='https://foo.bar.com',
                public=False,
                integration_type=VcsIntegrationTypes.github.value
            )
        ]
        result = api.import_repositories(organization.key, repositories)
        assert result['success']
        assert result['imported'] == 1

    def it_is_idempotent(self, setup_org):
        organization = setup_org
        repositories = [
            dict(
                name='A new repo',
                key=uuid.uuid4(),
                description='A new new repo',
                url='https://foo.bar.com',
                public=False,
                integration_type=VcsIntegrationTypes.github.value
            )
        ]
        # import once
        api.import_repositories(organization.key, repositories)
        # import again
        result = api.import_repositories(organization.key, repositories)
        assert result['success']
        assert result['imported'] == 1

    def it_updates_existing_repositories(self, setup_repo_org):
        repositories = [
            dict(
                name='A new repo',
                key=uuid.uuid4(),
                description='A new new repo',
                url='https://foo.bar.com',
                public=False,
                integration_type=VcsIntegrationTypes.github.value
            ),
            dict(
                name='$$$',
                key=rails_repository_key,
                description='A new new repo',
                public=False,
                url='https://foo.bar.com',
                integration_type=VcsIntegrationTypes.github.value
            )
        ]
        result = api.import_repositories(rails_organization_key, repositories)
        assert result['success']
        assert result['imported'] == 2
        assert db.connection().execute(
            f"Select name from analytics.repositories where key = '{rails_repository_key}'"
        ).scalar() == '$$$'
