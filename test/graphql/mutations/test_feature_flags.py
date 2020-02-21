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

class TestCreateFeatureFlag:

    def it_creates_a_new_feature_flag_given_name(self, create_feature_flag_fixture):
        name = test_feature_flags[0].get('name')
        # Using fixture just to get existing session
        _, session = create_feature_flag_fixture
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
        assert session.execute(
            f"select key from analytics.feature_flags where name='{name}'"
        ).scalar()

    def it_returns_error_message_when_feature_flag_already_exists(self, create_feature_flag_fixture):
        feature_flag,_ = create_feature_flag_fixture
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


class TestEnableFeatureFlag:

    def it_enables_feature_flag(self, create_feature_flag_fixture):
        feature_flag, _ = create_feature_flag_fixture
        client = Client(schema)
        feature_flag_key = feature_flag.key

        query = """
                    mutation enableFeatureFlag($enableFeatureFlagInput: EnableFeatureFlagInput! ){
                        enableFeatureFlag(
                            enableFeatureFlagInput: $enableFeatureFlagInput
                        ){
                            success,
                            errorMessage
                        }
                    }
                """
        response = client.execute(query, variable_values=dict(
            enableFeatureFlagInput=dict(
                featureFlagKey=feature_flag_key,
                enablements=enablements
            )
        ))
        assert 'data' in response
        assert response['data']['enableFeatureFlag']['success']

    def it_returns_error_message_for_invalid_feature_flag(self):
        client = Client(schema)
        feature_flag_key = uuid.uuid4()

        query = """
                    mutation enableFeatureFlag($enableFeatureFlagInput: EnableFeatureFlagInput! ){
                        enableFeatureFlag(
                            enableFeatureFlagInput: $enableFeatureFlagInput
                        ){
                            success,
                            errorMessage
                        }
                    }
                """
        response = client.execute(query, variable_values=dict(
            enableFeatureFlagInput=dict(
                featureFlagKey=feature_flag_key,
                enablements=enablements
            )
        ))
        assert 'data' in response
        assert response['data']['enableFeatureFlag']['errorMessage'] == "Failed to enable feature flag"

class TestUpdateEnablementsStatus:

    def it_updates_enablements_status(self, create_feature_flag_enablement_fixture):
        feature_flag, _ = create_feature_flag_enablement_fixture
        client = Client(schema)
        feature_flag_key = feature_flag.key

        enablement =  [dict(scopeKey=testScopeKey, enabled=false)]

        query = """
                    mutation updateEnablementsStatus($updateEnablementsStatusInput: UpdateEnablementsStatusInput! ){
                        updateEnablementsStatus(
                            updateEnablementsStatusInput: $updateEnablementsStatusInput
                        ){
                            success,
                            errorMessage
                        }
                    }
                """
        response = client.execute(query, variable_values=dict(
            updateEnablementsStatusInput=dict(
                featureFlagKey=feature_flag_key,
                enablements=enablement
            )
        ))
        assert 'data' in response
        assert response['data']['updateEnablementsStatus']['success']