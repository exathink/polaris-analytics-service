# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import pytest

from test.fixtures.graphql import *
from unittest.mock import patch
from polaris.common import db
from datetime import datetime

from graphene.test import Client
from polaris.analytics.service.graphql import schema
from polaris.analytics.service.graphql.viewer import Viewer

feature_flags_input = [
    dict(name='New Feature 1'),
    dict(name='New Feature 2'),
]

feature_flag_enablements_input = [
    dict(scope="account", scope_key=test_account_key, enabled=True),
    dict(scope="account", scope_key=test_account_key, enabled=False),
    dict(scope="user", scope_key=test_user_key, enabled=True),
    dict(scope="user", scope_key=test_user_key, enabled=False)
]


@pytest.yield_fixture()
def account_feature_flag_fixture_enable_all_true_enabled_true(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create(name=feature_flags_input[0]['name'])
        feature_flag.enable_all = True
        feature_flag.enable_all_date = datetime.utcnow()
        feature_flag.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[0])])
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def user_feature_flag_fixture_enable_all_true_enabled_true(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create(name=feature_flags_input[0]['name'])
        feature_flag.enable_all = True
        feature_flag.enable_all_date = datetime.utcnow()
        feature_flag.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[2])])
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def account_feature_flag_fixture_enable_all_true_enabled_false(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create(name=feature_flags_input[0]['name'])
        feature_flag.enable_all = True
        feature_flag.enable_all_date = datetime.utcnow()
        feature_flag.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[1])])
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def user_feature_flag_fixture_enable_all_true_enabled_false(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create(name=feature_flags_input[0]['name'])
        feature_flag.enable_all = True
        feature_flag.enable_all_date = datetime.utcnow()
        feature_flag.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[3])])
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def account_user_feature_flag_fixture_enable_all_true_enabled_null(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create(name=feature_flags_input[0]['name'])
        feature_flag.enable_all = True
        feature_flag.enable_all_date = datetime.utcnow()
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def account_feature_flag_fixture_enable_all_false_enabled_true(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create(name=feature_flags_input[0]['name'])
        feature_flag.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[0])])
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def user_feature_flag_fixture_enable_all_false_enabled_true(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create(name=feature_flags_input[0]['name'])
        feature_flag.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[2])])
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def account_feature_flag_fixture_enable_all_false_enabled_false(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create(name=feature_flags_input[0]['name'])
        feature_flag.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[1])])
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def user_feature_flag_fixture_enable_all_false_enabled_false(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create(name=feature_flags_input[0]['name'])
        feature_flag.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[3])])
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def account_user_feature_flag_fixture_enable_all_false_enabled_null(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create(name=feature_flags_input[0]['name'])
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def account_user_feature_flag_fixture_inactive_feature(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create(name=feature_flags_input[0]['name'])
        feature_flag.enable_all = True
        feature_flag.enable_all_date = datetime.utcnow()
        feature_flag.active = False
        feature_flag.deactivated_date = datetime.utcnow()
        feature_flag.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[0])])
        feature_flag.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[2])])
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def account_user_feature_flag_fixture_multiple_features_multiple_enablements_random(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flags = []
        # Feature flag 1
        feature_flag1 = FeatureFlag.create(name=feature_flags_input[0]['name'])
        feature_flag1.enable_all = True
        feature_flag1.enable_all_date = datetime.utcnow()
        feature_flag1.active = False
        feature_flag1.deactivated_date = datetime.utcnow()
        feature_flag1.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[0])])
        feature_flag1.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[3])])
        feature_flags.append(feature_flag1)
        # Feature flag 2
        feature_flag2 = FeatureFlag.create(name=feature_flags_input[1]['name'])
        feature_flag2.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[1])])
        feature_flag2.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[2])])
        feature_flags.append(feature_flag2)
        session.add_all(feature_flags)
    yield feature_flag1, feature_flag2


class TestAccountFeatureFlags:
    def it_returns_feature_flag_enablement_when_enable_all_true_enabled_true(self,
                                                                             account_feature_flag_fixture_enable_all_true_enabled_true):
        feature_flag = account_feature_flag_fixture_enable_all_true_enabled_true
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
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
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 1
        e1 = feature_flag_enablements[0]['node']
        assert e1['enabled']
        assert e1['key'] == str(feature_flag.key)

    def it_returns_feature_flag_enablement_when_enable_all_true_enabled_false(self,
                                                                              account_feature_flag_fixture_enable_all_true_enabled_false):
        feature_flag = account_feature_flag_fixture_enable_all_true_enabled_false
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
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
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 1
        e1 = feature_flag_enablements[0]['node']
        assert e1['enabled']
        assert e1['key'] == str(feature_flag.key)

    def it_returns_feature_flag_enablement_when_enable_all_true_enabled_null(self,
                                                                             account_user_feature_flag_fixture_enable_all_true_enabled_null):
        feature_flag = account_user_feature_flag_fixture_enable_all_true_enabled_null
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                         query getAccountFeatureFlagEnablements($account_key:String!) {
                             account(key: $account_key) {
                                 id
                                 featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                     edges {
                                         node {
                                             enabled
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
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 1
        e1 = feature_flag_enablements[0]['node']
        assert e1['enabled']
        assert e1['key'] == str(feature_flag.key)

    def it_returns_feature_flag_enablement_when_enable_all_false_enabled_true(self,
                                                                              account_feature_flag_fixture_enable_all_false_enabled_true):
        feature_flag = account_feature_flag_fixture_enable_all_false_enabled_true
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
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
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 1
        e1 = feature_flag_enablements[0]['node']
        assert e1['enabled']
        assert e1['key'] == str(feature_flag.key)

    def it_returns_feature_flag_enablement_when_enable_all_false_enabled_false(self,
                                                                               account_feature_flag_fixture_enable_all_false_enabled_false):
        feature_flag = account_feature_flag_fixture_enable_all_false_enabled_false
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
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
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 1
        e1 = feature_flag_enablements[0]['node']
        assert not e1['enabled']
        assert e1['key'] == str(feature_flag.key)

    def it_returns_feature_flag_enablement_when_enable_all_false_enabled_null(self,
                                                                              account_user_feature_flag_fixture_enable_all_false_enabled_null):
        _ = account_user_feature_flag_fixture_enable_all_false_enabled_null
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
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
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 0

    def it_returns_feature_flag_enablement_when_feature_flag_inactive(self,
                                                                      account_user_feature_flag_fixture_inactive_feature):
        _ = account_user_feature_flag_fixture_inactive_feature
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
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
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 0


current_user = dict(
    key=test_user_key,
    first_name='Test',
    last_name='User'
)


class objectview(object):
    def __init__(self, d):
        self.__dict__ = d


class TestUserFeatureFlags:

    def it_returns_feature_flag_enablement_when_enable_all_true_enabled_true(self,
                                                                             user_feature_flag_fixture_enable_all_true_enabled_true):
        feature_flag = user_feature_flag_fixture_enable_all_true_enabled_true
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getUserFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
                                            name
                                            key
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_user_key)
                                      )

        assert 'data' in response
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 1
        e1 = feature_flag_enablements[0]['node']
        assert e1['enabled']
        assert e1['key'] == str(feature_flag.key)

    def it_returns_feature_flag_enablement_when_enable_all_true_enabled_false(self,
                                                                              user_feature_flag_fixture_enable_all_true_enabled_false):
        feature_flag = user_feature_flag_fixture_enable_all_true_enabled_false
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getUserFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
                                            name
                                            key
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_user_key)
                                      )

        assert 'data' in response
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 1
        e1 = feature_flag_enablements[0]['node']
        assert e1['enabled']
        assert e1['key'] == str(feature_flag.key)

    def it_returns_feature_flag_enablement_when_enable_all_true_enabled_null(self,
                                                                             account_user_feature_flag_fixture_enable_all_true_enabled_null):
        feature_flag = account_user_feature_flag_fixture_enable_all_true_enabled_null
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getUserFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
                                            name
                                            key
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_user_key)
                                      )

        assert 'data' in response
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 1
        e1 = feature_flag_enablements[0]['node']
        assert e1['enabled']
        assert e1['key'] == str(feature_flag.key)

    def it_returns_feature_flag_enablement_when_enable_all_false_enabled_true(self,
                                                                              user_feature_flag_fixture_enable_all_false_enabled_true):
        feature_flag = user_feature_flag_fixture_enable_all_false_enabled_true
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getUserFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
                                            name
                                            key
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_user_key)
                                      )

        assert 'data' in response
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 1
        e1 = feature_flag_enablements[0]['node']
        assert e1['enabled']
        assert e1['key'] == str(feature_flag.key)

    def it_returns_feature_flag_enablement_when_enable_all_false_enabled_false(self,
                                                                               user_feature_flag_fixture_enable_all_false_enabled_false):
        feature_flag = user_feature_flag_fixture_enable_all_false_enabled_false
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getUserFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
                                            name
                                            key
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_user_key)
                                      )

        assert 'data' in response
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 1
        e1 = feature_flag_enablements[0]['node']
        assert not e1['enabled']
        assert e1['key'] == str(feature_flag.key)

    def it_returns_feature_flag_enablement_when_enable_all_false_enabled_null(self,
                                                                              account_user_feature_flag_fixture_enable_all_false_enabled_null):
        feature_flag = account_user_feature_flag_fixture_enable_all_false_enabled_null
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getUserFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
                                            name
                                            key
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_user_key)
                                      )

        assert 'data' in response
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 0

    def it_returns_feature_flag_enablement_when_feature_flag_inactive(self,
                                                                      account_user_feature_flag_fixture_inactive_feature):
        feature_flag = account_user_feature_flag_fixture_inactive_feature
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getUserFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                    edges {
                                        node {
                                            enabled
                                            name
                                            key
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_user_key)
                                      )

        assert 'data' in response
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 0

    def it_returns_feature_flag_enablement_when_multiple_feature_flags(self, account_user_feature_flag_fixture_multiple_features_multiple_enablements_random):
        feature_flag1, feature_flag2 = account_user_feature_flag_fixture_multiple_features_multiple_enablements_random
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                                query getUserFeatureFlagEnablements {
                                    viewer {
                                        id
                                        featureFlags(interfaces: [NamedNode, FeatureFlagEnablementInfo]) {
                                            edges {
                                                node {
                                                    enabled
                                                    name
                                                    key
                                                }
                                            }
                                        }
                                    }
                                }
                            """, variable_values=dict(account_key=test_user_key)
                                      )

        assert 'data' in response
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_enablements = result['featureFlags']['edges']
        assert len(feature_flag_enablements) == 1
        e1 = feature_flag_enablements[0]['node']
        assert not e1['enabled']
        assert e1['key'] == str(feature_flag2.key)
