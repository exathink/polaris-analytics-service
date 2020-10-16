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
from polaris.analytics.db.model import FeatureFlag, FeatureFlagEnablement

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


def create_feature_flags(feature_flags):
    with db.orm_session() as session:
        session.add_all(feature_flags)


@pytest.yield_fixture()
def feature_flag_fixture(org_repo_fixture, user_fixture,  cleanup):
    yield




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
def user_feature_flag_fixture_enable_all_false_enabled_true(org_repo_fixture, cleanup):
    _, _, _ = org_repo_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create(name=feature_flags_input[0]['name'])
        feature_flag.enablements.extend([FeatureFlagEnablement(**feature_flag_enablements_input[2])])
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


@pytest.yield_fixture()
def all_feature_flag_fixture_multiple_features_multiple_enablements(user_fixture, cleanup):
    _, _, _, user = user_fixture
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flags = []
        # Feature flag 1
        feature_flag1 = FeatureFlag.create(name='New Feature 1')
        feature_flag1.enable_all = True
        feature_flag1.enable_all_date = datetime.utcnow()
        feature_flag1.active = False
        feature_flag1.deactivated_date = datetime.utcnow()
        feature_flag1.enablements.extend(
            [FeatureFlagEnablement(scope="account", scope_key=test_account_key, enabled=True)])
        feature_flag1.enablements.extend([FeatureFlagEnablement(scope="user", scope_key=test_user_key, enabled=True)])
        feature_flags.append(feature_flag1)
        # Feature flag 2
        feature_flag2 = FeatureFlag.create(name='New Feature 2')
        feature_flag2.enablements.extend(
            [FeatureFlagEnablement(scope="account", scope_key=test_account_key, enabled=False)])
        feature_flag2.enablements.extend([FeatureFlagEnablement(scope="user", scope_key=test_user_key, enabled=True)])
        feature_flags.append(feature_flag2)
        session.add_all(feature_flags)
    yield feature_flag1, feature_flag2, user


class TestAccountFeatureFlags:

    def verify_feature_flag_is_enabled_when_enable_all_true_and_no_account_enablement_is_present(self,
                                                                                                 feature_flag_fixture):
        feature_flag = FeatureFlag.create(name="Feature 1")
        feature_flag.enable_all = True
        create_feature_flags([feature_flag])
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                         query getAccountFeatureFlagEnablements($account_key:String!) {
                             account(key: $account_key) {
                                 id
                                 featureFlags(interfaces: [NamedNode, Enablement]) {
                                     edges {
                                         node {
                                             name
                                             key
                                             enabled
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
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag.key)
        assert feature_flag_nodes[0]['node']['enabled']

    def verify_feature_flag_is_enabled_when_enable_all_is_true_and_account_enabled_is_true(self,
                                                                                           feature_flag_fixture):
        feature_flag = FeatureFlag.create(name="Feature 1")
        feature_flag.enable_all = True
        feature_flag.enablements.extend([
            FeatureFlagEnablement(scope="account", scope_key=test_account_key, enabled=True)
        ])
        create_feature_flags([feature_flag])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
                                    edges {
                                        node {
                                            id
                                            name
                                            key
                                            enabled
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
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag.key)
        assert feature_flag_nodes[0]['node']['enabled']

    def verify_feature_flag_is_enabled_when_enable_all_is_true_and_account_enabled_is_false(self,
                                                                                            feature_flag_fixture):
        feature_flag = FeatureFlag.create(name="Feature 1")
        feature_flag.enable_all = True
        feature_flag.enablements.extend([
            FeatureFlagEnablement(scope="account", scope_key=test_account_key, enabled=False)
        ])
        create_feature_flags([feature_flag])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
                                    edges {
                                        node {
                                            name
                                            key
                                            enabled
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
        feature_flag_nodes = result['featureFlags']['edges']

        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag.key)
        assert feature_flag_nodes[0]['node']['enabled']

    def verify_feature_flag_is_enabled_when_enable_all_is_false_and_account_enabled_is_true(self,
                                                                                            feature_flag_fixture):
        feature_flag = FeatureFlag.create(name="Feature 1")
        feature_flag.enable_all = False
        feature_flag.enablements.extend([
            FeatureFlagEnablement(scope="account", scope_key=test_account_key, enabled=True)
        ])
        create_feature_flags([feature_flag])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
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
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag.key)
        assert feature_flag_nodes[0]['node']['enabled']

    def verify_feature_flag_is_not_enabled_when_enable_all_is_false_and_account_enabled_is_false(self,
                                                                                                 feature_flag_fixture):
        feature_flag = FeatureFlag.create(name="Feature 1")
        feature_flag.enable_all = False
        feature_flag.enablements.extend([
            FeatureFlagEnablement(scope="account", scope_key=test_account_key, enabled=False)
        ])
        create_feature_flags([feature_flag])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
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
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag.key)
        assert not feature_flag_nodes[0]['node']['enabled']

    def verify_feature_flag_enabled_is_null_when_enable_all_is_false_and_there_is_no_account_enablement_record(self,
                                                                                                               feature_flag_fixture):
        feature_flag = FeatureFlag.create(name="Feature 1")
        feature_flag.enable_all = False
        create_feature_flags([feature_flag])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
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
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag.key)
        assert feature_flag_nodes[0]['node']['enabled'] is None

    def verify_that_inactive_feature_flags_are_not_returned(self, feature_flag_fixture):
        feature_flag_0 = FeatureFlag.create(name="Feature 1")
        feature_flag_0.active = True

        feature_flag_1 = FeatureFlag.create(name="Feature 2")
        feature_flag_1.active = False

        create_feature_flags([feature_flag_0, feature_flag_1])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
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
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag_0.key)

    def verify_that_the_correct_feature_flags_are_enabled_when_there_are_multiple_feature_flags(self,
                                                                                                feature_flag_fixture):
        feature_flag_0 = FeatureFlag.create(name="Feature 1")
        feature_flag_0.enable_all = True

        feature_flag_1 = FeatureFlag.create(name="Feature 2")
        feature_flag_1.enable_all = False
        feature_flag_1.enablements.extend([
            FeatureFlagEnablement(scope="account", scope_key=test_account_key, enabled=False)
        ])

        create_feature_flags([feature_flag_0, feature_flag_1])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.account.Account.check_access', return_value=True):
            response = client.execute("""
                        query getAccountFeatureFlagEnablements($account_key:String!) {
                            account(key: $account_key) {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
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
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 2
        assert {(edge['node']['key'], edge['node']['enabled']) for edge in feature_flag_nodes} == {
            (str(feature_flag_0.key), True),
            (str(feature_flag_1.key), False)
        }



current_user = dict(
    key=test_user_key,
    first_name='Test',
    last_name='User'
)


class objectview(object):
    def __init__(self, d):
        self.__dict__ = d


class TestUserFeatureFlags:

    def verify_feature_flag_is_enabled_when_enable_all_true_and_no_user_enablement_is_present(self,
                                                                                                 feature_flag_fixture):
        feature_flag = FeatureFlag.create(name="Feature 1")
        feature_flag.enable_all = True
        create_feature_flags([feature_flag])
        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                         query getViewerFeatureFlagEnablements {
                             viewer {
                                 id
                                 featureFlags(interfaces: [NamedNode, Enablement]) {
                                     edges {
                                         node {
                                             name
                                             key
                                             enabled
                                         }
                                     }
                                 }
                             }
                         }
                     """, variable_values=dict(account_key=test_account_key)
                                      )

        assert 'data' in response
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag.key)
        assert feature_flag_nodes[0]['node']['enabled']

    def verify_feature_flag_is_enabled_when_enable_all_is_true_and_user_enabled_is_true(self,
                                                                                           feature_flag_fixture):
        feature_flag = FeatureFlag.create(name="Feature 1")
        feature_flag.enable_all = True
        feature_flag.enablements.extend([
            FeatureFlagEnablement(scope="user", scope_key=test_user_key, enabled=True)
        ])
        create_feature_flags([feature_flag])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getViewerFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
                                    edges {
                                        node {
                                            id
                                            name
                                            key
                                            enabled
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_account_key)
                                      )

        assert 'data' in response
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag.key)
        assert feature_flag_nodes[0]['node']['enabled']

    def verify_feature_flag_is_enabled_when_enable_all_is_true_and_user_enabled_is_false(self,
                                                                                            feature_flag_fixture):
        feature_flag = FeatureFlag.create(name="Feature 1")
        feature_flag.enable_all = True
        feature_flag.enablements.extend([
            FeatureFlagEnablement(scope="user", scope_key=test_user_key, enabled=False)
        ])
        create_feature_flags([feature_flag])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getViewerFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
                                    edges {
                                        node {
                                            name
                                            key
                                            enabled
                                        }
                                    }
                                }
                            }
                        }
                    """, variable_values=dict(account_key=test_account_key)
                                      )

        assert 'data' in response
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_nodes = result['featureFlags']['edges']

        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag.key)
        assert feature_flag_nodes[0]['node']['enabled']

    def verify_feature_flag_is_enabled_when_enable_all_is_false_and_account_enabled_is_true(self,
                                                                                            feature_flag_fixture):
        feature_flag = FeatureFlag.create(name="Feature 1")
        feature_flag.enable_all = False
        feature_flag.enablements.extend([
            FeatureFlagEnablement(scope="user", scope_key=test_user_key, enabled=True)
        ])
        create_feature_flags([feature_flag])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getViewerFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
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
        result = response['data']['viewer']
        assert result
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag.key)
        assert feature_flag_nodes[0]['node']['enabled']

    def verify_feature_flag_is_not_enabled_when_enable_all_is_false_and_account_enabled_is_false(self,
                                                                                                 feature_flag_fixture):
        feature_flag = FeatureFlag.create(name="Feature 1")
        feature_flag.enable_all = False
        feature_flag.enablements.extend([
            FeatureFlagEnablement(scope="user", scope_key=test_user_key, enabled=False)
        ])
        create_feature_flags([feature_flag])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getViewerFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
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
        result = response['data']['viewer']
        assert result
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag.key)
        assert not feature_flag_nodes[0]['node']['enabled']

    def verify_feature_flag_enabled_is_null_when_enable_all_is_false_and_there_is_no_account_enablement_record(self,
                                                                                                               feature_flag_fixture):
        feature_flag = FeatureFlag.create(name="Feature 1")
        feature_flag.enable_all = False
        create_feature_flags([feature_flag])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getViewerFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
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
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag.key)
        assert feature_flag_nodes[0]['node']['enabled'] is None

    def verify_that_inactive_feature_flags_are_not_returned(self, feature_flag_fixture):
        feature_flag_0 = FeatureFlag.create(name="Feature 1")
        feature_flag_0.active = True

        feature_flag_1 = FeatureFlag.create(name="Feature 2")
        feature_flag_1.active = False

        create_feature_flags([feature_flag_0, feature_flag_1])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getViewerFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
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
        result = response['data']['viewer']
        assert result
        assert result['id']
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 1
        assert feature_flag_nodes[0]['node']['key'] == str(feature_flag_0.key)

    def verify_that_the_correct_feature_flags_are_enabled_when_there_are_multiple_feature_flags(self,
                                                                                                feature_flag_fixture):
        feature_flag_0 = FeatureFlag.create(name="Feature 1")
        feature_flag_0.enable_all = True

        feature_flag_1 = FeatureFlag.create(name="Feature 2")
        feature_flag_1.enable_all = False
        feature_flag_1.enablements.extend([
            FeatureFlagEnablement(scope="user", scope_key=test_user_key, enabled=False)
        ])

        create_feature_flags([feature_flag_0, feature_flag_1])

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.get_viewer',
                   return_value=Viewer(objectview(current_user))):
            response = client.execute("""
                        query getViewerFeatureFlagEnablements {
                            viewer {
                                id
                                featureFlags(interfaces: [NamedNode, Enablement]) {
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
        result = response['data']['viewer']
        assert result
        feature_flag_nodes = result['featureFlags']['edges']
        assert len(feature_flag_nodes) == 2
        assert {(edge['node']['key'], edge['node']['enabled']) for edge in feature_flag_nodes} == {
            (str(feature_flag_0.key), True),
            (str(feature_flag_1.key), False)
        }



class TestAllFeatureFlags:
    def it_returns_all_feature_flag_nodes_info(self,
                                               feature_flag_fixture):
        feature_flag_0 = FeatureFlag.create(name="Feature 1")
        feature_flag_0.enable_all = True

        feature_flag_1 = FeatureFlag.create(name="Feature 2")
        feature_flag_1.enable_all = False
        feature_flag_1.enablements.extend([
            FeatureFlagEnablement(scope="user", scope_key=test_user_key, enabled=False)
        ])

        create_feature_flags([feature_flag_0, feature_flag_1])
        client = Client(schema)
        response = client.execute("""
                                query getAllFeatureFlags {
                                    allFeatureFlags {
                                        edges {
                                            node {
                                                id
                                                name
                                                key
                                                enableAll
                                                }
                                            }
                                        }    
                                    }
                                
                            """
                                  )
        assert 'data' in response
        result = response['data']['allFeatureFlags']['edges']
        assert len(result) == 2
        assert any([result[0]['node']['enableAll'], result[1]['node']['enableAll']])
        assert str(feature_flag_0.key) in [r['node']['key'] for r in result]
        assert str(feature_flag_1.key) in [r['node']['key'] for r in result]

    def it_returns_all_feature_flag_enablements_info(self,
                                                     feature_flag_fixture):
        feature_flag_0 = FeatureFlag.create(name="Feature 1")
        feature_flag_0.enablements.extend([
            FeatureFlagEnablement(scope="account", scope_key=test_account_key, enabled=False),
            FeatureFlagEnablement(scope="user", scope_key=test_user_key, enabled=False)
        ])
        feature_flag_0.enable_all = True

        feature_flag_1 = FeatureFlag.create(name="Feature 2")
        feature_flag_1.enable_all = False
        feature_flag_1.enablements.extend([
            FeatureFlagEnablement(scope="user", scope_key=test_user_key, enabled=False)
        ])
        create_feature_flags([feature_flag_0, feature_flag_1])

        client = Client(schema)
        response = client.execute("""
                                query getAllFeatureFlags {
                                    allFeatureFlags(interfaces: [FeatureFlagEnablements]) {
                                        edges {
                                            node {
                                                id
                                                name
                                                key
                                                enableAll
                                                enablements {
                                                    scope
                                                    scopeKey
                                                    enabled
                                                    scopeRefName
                                                }
                                            }
                                        }    
                                    }
                                }
                            """
                                  )
        assert 'data' in response
        edges = response['data']['allFeatureFlags']['edges']
        assert len(edges) == 2
        ff0 = find(edges, lambda edge: edge['node']['key'] == str(feature_flag_0.key))

        assert dict(ff0['node']['enablements'][0]) == dict(scope='account', scopeKey=str(uuid.UUID(test_account_key)),
                                                     enabled=True, scopeRefName='test-account')
        assert dict(ff0['node']['enablements'][1]) == dict(scope='user', scopeKey=str(uuid.UUID(test_user_key)),
                                                           enabled=True, scopeRefName='Test User')


        ff1 = find(edges, lambda edge: edge['node']['key'] == str(feature_flag_1.key))
        assert dict(ff1['node']['enablements'][0]) == dict(scope='user', scopeKey=str(uuid.UUID(test_user_key)),
                                                           enabled=False, scopeRefName='Test User')


