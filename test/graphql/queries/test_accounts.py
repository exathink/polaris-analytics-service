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

from graphene.test import Client
from polaris.analytics.service.graphql import schema

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
