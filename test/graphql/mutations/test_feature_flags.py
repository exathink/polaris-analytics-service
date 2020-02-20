# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest
import uuid
from polaris.common import db
from polaris.analytics.db.model import FeatureFlag
from graphene.test import Client
from polaris.analytics.service.graphql import schema


from test.fixtures.graphql import *
from test.constants import *

test_feature_flags = [
    dict(name='Test Feature Flag 1'),
    dict(name='Test Feature Flag 2')
]


class TestCreateFeatureFlag:

    def it_create_a_new_feature_flag_given_name(self):
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
        name = create_feature_flag_fixture
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
