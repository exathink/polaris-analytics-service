# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from polaris.analytics.db.model import Repository, WorkItemsSource
from test.fixtures.repo_org import *

@pytest.fixture
def setup_work_items_source_repo_resolution(setup_org):
    organization = setup_org
    repo_source_id = str(uuid.uuid4())
    with db.orm_session() as session:
        session.add(organization)
        repository = Repository(
            name='rails',
            key=uuid.uuid4(),
            source_id=repo_source_id
        )
        organization.repositories.append(
            repository
        )

    yield organization, repository


@pytest.fixture
def setup_repo_work_items_source_resolution(setup_org):
    organization = setup_org
    repo_source_id = str(uuid.uuid4())

    with db.orm_session() as session:
        session.add(organization)
        work_items_source = WorkItemsSource(
            organization_key=organization.key,
            key=uuid.uuid4(),
            name='foo',
            integration_type='github',
            work_items_source_type='repository_issues',
            commit_mapping_scope='repository',
            source_id=repo_source_id
        )
        organization.work_items_sources.append(
            work_items_source
        )

    yield organization, work_items_source
