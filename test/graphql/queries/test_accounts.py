# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest

from test.fixtures.graphql import *
from test.constants import *
from unittest.mock import patch
from polaris.common import db
from sqlalchemy import and_
from graphene.test import Client
from polaris.analytics.service.graphql import schema
from polaris.analytics.db.model import Contributor, ContributorAlias, repositories_contributor_aliases


class TestAccount:

    def it_implements_the_named_node_interface(self, org_repo_fixture):
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountNode($account_key:String!) {
                            account(key: $account_key) {
                                id
                                name
                                key
                            }
                        }
                    """, variable_values=dict(account_key=test_account_key)
                                      )

        assert 'data' in response
        result = response['data']['account']
        assert result
        assert result['id']
        assert result['name']
        assert result['key']

    def it_implements_the_account_info_interface(self, org_repo_fixture):
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountNode($account_key:String!) {
                            account(key: $account_key, interfaces: [AccountInfo]) {
                                ... on AccountInfo {
                                    created
                                    updated
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_account_key)
                                      )

        assert 'data' in response
        result = response['data']['account']
        assert result
        assert result['created']
        assert result['updated']

    def it_implements_the_owner_info_interface(self, org_repo_fixture):
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountNode($account_key:String!) {
                            account(key: $account_key, interfaces: [AccountInfo]) {
                                ... on OwnerInfo {
                                    ownerKey
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_account_key)
                                      )

        assert 'data' in response
        result = response['data']['account']
        assert result
        assert result['ownerKey'] == str(uuid.UUID(test_user_key))

    def it_resolves_organizations(self, org_repo_fixture):
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountNode($account_key:String!) {
                            account(key: $account_key) {
                                organizations {
                                    edges {
                                        node {
                                            id
                                            name
                                            key
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_account_key)
                                      )

        assert 'data' in response
        result = response['data']['account']
        organizations = result['organizations']['edges']
        assert organizations
        assert len(organizations) == 1
        assert organizations[0]['node']['id']
        assert organizations[0]['node']['name']
        assert organizations[0]['node']['key']

    def it_resolves_organization_project_and_repository_count(self, org_repo_fixture):
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountNode($account_key:String!) {
                            account(key: $account_key) {
                                organizations(interfaces: [RepositoryCount, ProjectCount]) {
                                    edges {
                                        node {
                                            projectCount
                                            repositoryCount                                            
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_account_key)
                                      )

        assert 'data' in response
        result = response['data']['account']
        organizations = result['organizations']['edges']
        assert organizations
        assert len(organizations) == 1
        assert organizations[0]['node']['projectCount'] == 2
        assert organizations[0]['node']['repositoryCount'] == 4

    def it_resolves_projects(self, org_repo_fixture):
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountNode($account_key:String!) {
                            account(key: $account_key) {
                                projects {
                                    edges {
                                        node {
                                            id
                                            name
                                            key
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_account_key)
                                      )

        assert 'data' in response
        result = response['data']['account']
        projects = result['projects']['edges']
        assert projects
        assert len(projects) == 2
        for project in projects:
            assert project['node']['id']
            assert project['node']['key']
            assert project['node']['name']

    def it_resolves_project_repository_count(self, org_repo_fixture):
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountNode($account_key:String!) {
                            account(key: $account_key) {
                                projects(interfaces: [RepositoryCount]){
                                    edges {
                                        node {
                                            repositoryCount
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_account_key)
                                      )

        assert 'data' in response
        result = response['data']['account']
        projects = result['projects']['edges']
        assert projects
        assert len(projects) == 2
        for project in projects:
            assert project['node']['repositoryCount'] == 2

    def it_implements_account_contributor_connection(self, commits_fixture):
        organization, projects, repositories, contributor = commits_fixture
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountContributorNodes($account_key:String!) {
                            account(key: $account_key) {
                                contributors{
                                    edges {
                                        node {
                                            key
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_account_key)
                                      )

            assert 'data' in response
            result = response['data']['account']
            contributors = result['contributors']['edges']
            assert contributors
            assert len(contributors) == 1
            for contributor in contributors:
                assert uuid.UUID(contributor['node']['key']).hex == test_contributor_key


class ContributorImportApiHelper:

    def __init__(self, repositories, contributors_info):
        self.repositories = repositories
        self.contributors_info = contributors_info

    def import_contributors(self):
        contributor_objects = []
        contributor_alias_objects = []
        with db.orm_session() as session:
            for info in self.contributors_info:
                contributor = Contributor(
                        key=info['key'],
                        name=info['name']
                )
                session.add(contributor)
                session.flush()
                contributor_objects.append(contributor)
                contributor_alias = ContributorAlias(
                        key=info['key'],
                        name=info['name'],
                        source_alias=info['source_alias'],
                        contributor_id=contributor.id,
                        source='vcs',
                        robot=False
                )
                session.add(contributor_alias)
                contributor_alias_objects.append(contributor_alias)

        return contributor_objects, contributor_alias_objects

    def update_contributor(self, contributor_key, update_dict):
        pass

    def update_contributor_alias(self, alias_key, update_dict):
        pass

    def add_repository_contributor_alias(self, mapping, join_this=None):
        with db.orm_session(join_this) as session:
            session.connection().execute(
                repositories_contributor_aliases.insert(
                    mapping
            )
        )

    def update_repository_contributor_alias(self, repository_id, contributor_alias_id, updated_mapping, join_this=None):
        with db.orm_session(join_this) as session:
            session.connection().execute(
                repositories_contributor_aliases.update().where(
                    and_(
                        repositories_contributor_aliases.c.repository_id==repository_id,
                        repositories_contributor_aliases.c.contributor_alias_id==contributor_alias_id
                    )
                ).values(
                    updated_mapping
                )
            )


class TestAccountContributorsConnection:

    @pytest.yield_fixture()
    def setup(self, org_repo_fixture):
        organization, projects, repositories = org_repo_fixture
        contributor_aliases_info = [
            dict(
                key=uuid.uuid4(),
                name='Joe Blow',
                source_alias='joe@blow.com'
            ),
            dict(
                key=uuid.uuid4(),
                name='Joe Blow',
                source_alias='joe@local-macbook-pro'
            ),
            dict(
                key=uuid.uuid4(),
                name='Ida J',
                source_alias='ida@jay.com'
            )
        ]

        api_helper = ContributorImportApiHelper(repositories, contributor_aliases_info)

        query = """
                    query getAccountContributorNodes($account_key:String!) {
                        account(key: $account_key) {
                            contributors(interfaces:[CommitSummary, ContributorAliasesInfo], commitWithinDays:20){
                                edges {
                                    node {
                                        id
                                        key
                                        name
                                        earliestCommit
                                        latestCommit
                                        commitCount
                                        contributorAliasesInfo {
                                            key
                                            name
                                            alias
                                            latestCommit
                                            earliestCommit
                                            commitCount
                                        }
                                    }
                                }
                            }
                        }
                    }
                """

        yield Fixture(
            repositories=repositories,
            contributors=contributors,
            contributor_aliases=contributor_aliases,
            api_helper=api_helper,
            query=query
        )

    class TestWithNoContributors:

        class TestWithoutFilterWithoutInterface:

            @pytest.yield_fixture()
            def setup(self, setup):
                fixture = setup
                plain_query = """
                            query getAccountContributorNodes($account_key:String!) {
                                account(key: $account_key) {
                                    contributors{
                                        edges {
                                            node {
                                                key
                                            }
                                        }
                                    }
                                }
                            }
                        """
                yield Fixture(
                    parent=fixture,
                    plain_query=plain_query
                )

            def it_returns_no_contributors_when_there_are_none(self, setup):
                fixture = setup
                client = Client(schema)

                with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
                    response = client.execute(fixture.query, variable_values=dict(account_key=test_account_key))

                    assert 'data' in response
                    result = response['data']['account']
                    assert len(result['contributors']['edges']) == 0

        class TestWithFilterWithInterface:

            def it_returns_no_contributors_when_there_are_none(self, setup):
                fixture = setup
                client = Client(schema)

                with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
                    response = client.execute(fixture.query, variable_values=dict(account_key=test_account_key))

                    assert 'data' in response
                    result = response['data']['account']
                    assert len(result['contributors']['edges']) == 0

    class TestWithContributorsWithSingleAlias:

        @pytest.yield_fixture()
        def setup(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            contributor_objects, contributor_alias_objects = api_helper.import_contributors()
            # Add repository_contributor_aliases mappings
            api_helper.add_repository_contributor_alias(
                dict(
                    repository_id=fixture.repositories['alpha'].id,
                    contributor_alias_id=contributor_alias_objects[0].id,
                    earliest_commit=get_date("2018-12-03"),
                    latest_commit=datetime.utcnow() - timedelta(days=10),
                    commit_count=200,
                    contributor_id=contributor_objects[0].id,
                    robot=False
                )
            )
            api_helper.add_repository_contributor_alias(
                dict(
                    repository_id=fixture.repositories['alpha'].id,
                    contributor_alias_id=contributor_alias_objects[1].id,
                    earliest_commit=get_date("2018-12-03"),
                    latest_commit=datetime.utcnow() - timedelta(days=10),
                    commit_count=100,
                    contributor_id=contributor_objects[1].id,
                    robot=False
                )
            )
            api_helper.add_repository_contributor_alias(
                dict(
                    repository_id=fixture.repositories['alpha'].id,
                    contributor_alias_id=contributor_alias_objects[2].id,
                    earliest_commit=get_date("2018-12-03"),
                    latest_commit=datetime.utcnow() - timedelta(days=10),
                    commit_count=300,
                    contributor_id=contributor_objects[2].id,
                    robot=False
                )
            )

            yield Fixture(
                parent=fixture
            )

        def it_returns_all_contributors_with_their_aliases(self, setup):
            fixture = setup
            client = Client(schema)

            with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
                response = client.execute(fixture.query, variable_values=dict(account_key=test_account_key))

                assert 'data' in response
                result = response['data']['account']
                assert len(result['contributors']['edges']) == 3
                contributors = result['contributors']['edges']
                for c_node in contributors:
                    contributor = c_node['node']
                    assert contributor['id']
                    assert contributor['key'] == contributor['contributorAliasesInfo'][0]['key']
                    assert contributor['name'] == contributor['contributorAliasesInfo'][0]['name']
                    assert contributor['latestCommit'] == contributor['contributorAliasesInfo'][0]['latestCommit']
                    assert contributor['earliestCommit'] == contributor['contributorAliasesInfo'][0]['earliestCommit']
                    assert contributor['commitCount'] == contributor['contributorAliasesInfo'][0]['commitCount']

        class TestWithFilterWithoutInterface:
            pass

    class TestWithFilterWithContributorAliasesInterface:
        pass

    class TestWithFilterWithCommitSummaryInterface:
        pass

    class TestWithFilterWithCommitSummaryContributorAliasesInterface:
        pass
