# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest
import uuid

from sqlalchemy import true, false

from polaris.common import db
from graphene.test import Client
from polaris.analytics.service.graphql import schema

from test.fixtures.graphql import *
from test.constants import *

test_feature_flags = [
    dict(name='Test Feature Flag 1'),
    dict(name='Test Feature Flag 2')
]

testScopeKey = uuid.uuid4()
enablements = [
    dict(scope="user", scopeKey=testScopeKey, enabled=true),
    dict(scope="user", scopeKey=uuid.uuid4(), enabled=false),
    dict(scope="account", scopeKey=uuid.uuid4(), enabled=false)
]

test_scope_key = uuid.uuid4()
enablementsInput = [
    dict(scope="user", scope_key=test_scope_key, enabled=True),
    dict(scope="user", scope_key=uuid.uuid4(), enabled=False),
    dict(scope="account", scope_key=uuid.uuid4(), enabled=False)
]


class TestCreateFeatureFlag:

    def it_creates_a_new_feature_flag_given_name(self, create_feature_flag_fixture):
        name = test_feature_flags[0].get('name')

        client = Client(schema)

        response = client.execute("""
                    mutation CreateFeatureFlag($createFeatureFlagInput: CreateFeatureFlagInput!) {
                        createFeatureFlag(createFeatureFlagInput:$createFeatureFlagInput) 
                        {
                        success
                        errorMessage
                        featureFlag {
                                id
                                name
                                key
                            }
                        }
                    }
                """, variable_values=dict(
            createFeatureFlagInput=dict(
                name=name
            )
        ))
        assert response['data']
        assert response['data']['createFeatureFlag']['success']
        assert response['data']['createFeatureFlag']['featureFlag']['name'] == name
        assert db.connection().execute(
            f"select key from analytics.feature_flags where name='{name}'"
        ).scalar()

    def it_returns_error_message_when_feature_flag_already_exists(self, create_feature_flag_fixture):
        feature_flag = create_feature_flag_fixture
        name = feature_flag.name
        client = Client(schema)

        response = client.execute("""
                            mutation CreateFeatureFlag($createFeatureFlagInput: CreateFeatureFlagInput!) {
                                createFeatureFlag(createFeatureFlagInput:$createFeatureFlagInput)
                                {
                                    success
                                    errorMessage
                                    featureFlag {
                                        id
                                        name
                                        key
                                    }
                                }
                            }
                        """, variable_values=dict(
            createFeatureFlagInput=dict(
                name=name
            )
        ))
        assert response['data']
        assert not response['data']['createFeatureFlag']['success']
        assert response['data']['createFeatureFlag']['errorMessage'] == f'Feature flag {name} already exists'


@pytest.yield_fixture()
def create_feature_flag_fixture(cleanup):
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create("Test Feature Flag")
        session.add(feature_flag)
    yield feature_flag


@pytest.yield_fixture()
def create_feature_flag_enablement_fixture(cleanup):
    with db.orm_session() as session:
        session.expire_on_commit = False
        feature_flag = FeatureFlag.create("New Feature")
        feature_flag.enablements.extend([
            FeatureFlagEnablement(**item)
            for item in enablementsInput
        ])
        session.add(feature_flag)
    yield feature_flag


class TestCreateFeatureFlagEnablement:

    def it_creates_feature_flag_enablement(self, create_feature_flag_fixture):
        feature_flag = create_feature_flag_fixture
        client = Client(schema)
        feature_flag_key = feature_flag.key

        query = """
                    mutation updateFeatureFlag($updateFeatureFlagInput: UpdateFeatureFlagInput! ){
                        updateFeatureFlag(
                            updateFeatureFlagInput: $updateFeatureFlagInput
                        ){
                            success,
                            errorMessage
                        }
                    }
                """
        response = client.execute(query, variable_values=dict(
            updateFeatureFlagInput=dict(
                key=feature_flag_key,
                active=True,
                enableAll=False,
                enablements=enablements
            )
        ))
        assert 'data' in response
        assert response['data']['updateFeatureFlag']['success']

    def it_returns_error_message_for_invalid_feature_flag(self):
        client = Client(schema)
        feature_flag_key = uuid.uuid4()

        query = """
                    mutation updateFeatureFlag($updateFeatureFlagInput: UpdateFeatureFlagInput! ){
                        updateFeatureFlag(
                            updateFeatureFlagInput: $updateFeatureFlagInput
                        ){
                            success,
                            errorMessage
                        }
                    }
                """
        response = client.execute(query, variable_values=dict(
            updateFeatureFlagInput=dict(
                key=feature_flag_key,
                active=True,
                enableAll=False,
                enablements=enablements
            )
        ))
        assert 'data' in response
        assert response['data']['updateFeatureFlag']['errorMessage']


class TestUpdateEnablementsStatus:

    def it_updates_enablements_status(self, create_feature_flag_enablement_fixture):
        feature_flag = create_feature_flag_enablement_fixture
        client = Client(schema)
        feature_flag_key = feature_flag.key

        enablement = [dict(scope="user", scopeKey=test_scope_key, enabled=false)]

        query = """
                    mutation updateFeatureFlag($updateFeatureFlagInput: UpdateFeatureFlagInput! ){
                        updateFeatureFlag(
                            updateFeatureFlagInput: $updateFeatureFlagInput
                        ){
                            success,
                            errorMessage
                        }
                    }
                """
        response = client.execute(query, variable_values=dict(
            updateFeatureFlagInput=dict(
                key=feature_flag_key,
                active=True,
                enableAll=False,
                enablements=enablement
            )
        ))
        assert 'data' in response
        assert response['data']['updateFeatureFlag']['success']


class TestEnableFeatureFlag:

    def it_enables_feature_flag(self, create_feature_flag_fixture):
        feature_flag = create_feature_flag_fixture
        client = Client(schema)
        feature_flag_key = feature_flag.key

        query = """
                    mutation updateFeatureFlag($updateFeatureFlagInput: UpdateFeatureFlagInput! ){
                        updateFeatureFlag(
                            updateFeatureFlagInput: $updateFeatureFlagInput
                        ){
                            success,
                            errorMessage
                        }
                    }
                """
        response = client.execute(query, variable_values=dict(
            updateFeatureFlagInput=dict(
                key=feature_flag_key,
                enableAll=True,
                active=True,
                enablements=None
            )
        ))
        assert 'data' in response
        assert response['data']['updateFeatureFlag']['success']
        featureFlag = db.connection().execute(
            f"select * from analytics.feature_flags where key='{feature_flag_key}'"
        ).fetchone()
        assert featureFlag.enable_all
        assert featureFlag.enable_all_date is not None

    def it_disables_feature_flag(self, create_feature_flag_fixture):
        feature_flag = create_feature_flag_fixture
        client = Client(schema)
        feature_flag_key = feature_flag.key

        query = """
                    mutation updateFeatureFlag($updateFeatureFlagInput: UpdateFeatureFlagInput! ){
                        updateFeatureFlag(
                            updateFeatureFlagInput: $updateFeatureFlagInput
                        ){
                            success,
                            errorMessage
                        }
                    }
                """
        response = client.execute(query, variable_values=dict(
            updateFeatureFlagInput=dict(
                key=feature_flag_key,
                enableAll=False,
                active=False,
                enablements=None
            )
        ))
        assert 'data' in response
        assert response['data']['updateFeatureFlag']['success']
        featureFlag = db.connection().execute(
            f"select * from analytics.feature_flags where key='{feature_flag_key}'"
        ).fetchone()
        assert not featureFlag.enable_all
        assert featureFlag.enable_all_date is None

    def it_returns_error_for_invalid_feature_flag(self):
        client = Client(schema)
        feature_flag_key = uuid.uuid4()

        query = """
                    mutation updateFeatureFlag($updateFeatureFlagInput: UpdateFeatureFlagInput! ){
                        updateFeatureFlag(
                            updateFeatureFlagInput: $updateFeatureFlagInput
                        ){
                            success,
                            errorMessage
                        }
                    }
                """
        response = client.execute(query, variable_values=dict(
            updateFeatureFlagInput=dict(
                key=feature_flag_key,
                enableAll=True,
                active=True,
                enablements=None
            )
        ))
        assert 'data' in response
        assert response['data']['updateFeatureFlag']['errorMessage']


class TestDeactivateFeatureFlag:

    def it_deactivates_feature_flag(self, create_feature_flag_fixture):
        feature_flag = create_feature_flag_fixture
        client = Client(schema)
        feature_flag_key = feature_flag.key

        query = """
                    mutation updateFeatureFlag($updateFeatureFlagInput: UpdateFeatureFlagInput! ){
                        updateFeatureFlag(
                            updateFeatureFlagInput: $updateFeatureFlagInput
                        ){
                            success,
                            errorMessage
                        }
                    }
                """
        response = client.execute(query, variable_values=dict(
            updateFeatureFlagInput=dict(
                key=feature_flag_key,
                active=False,
                enableAll=False,
                enablements=None
            )
        ))
        assert 'data' in response
        assert response['data']['updateFeatureFlag']['success']
        featureFlag = db.connection().execute(
            f"select * from analytics.feature_flags where key='{feature_flag_key}'"
        ).fetchone()
        assert not featureFlag.active
