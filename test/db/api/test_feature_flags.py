# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Maneet Sehgal

from polaris.analytics.db import api, model
from polaris.common import db
import uuid


class objectview(object):
    def __init__(self, d):
        self.__dict__ = d


test_scope_key = uuid.uuid4()


class TestCreateFeatureFlag:
    def it_creates_feature_flag(self):
        name = 'Test feature flag'
        feature_flag = model.FeatureFlag(
            name=name
        )
        result = api.create_feature_flag(feature_flag)
        assert result['success']
        assert db.connection().execute(
            f"select key from analytics.feature_flags where name='{name}'"
        ).scalar()


class TestCreateFeatureFlagEnablement:
    def it_creates_feature_flag_enablement(self):
        name = 'Test feature flag'
        feature_flag = db.connection().execute(
            f"select * from analytics.feature_flags where name='{name}'"
        ).fetchone()

        feature_flag_enablement_input = dict(
            key=feature_flag.key,
            active=True,
            enable_all=False,
            enablements=[
                dict(
                    scope="user",
                    scope_key=test_scope_key,
                    enabled=True
                ),
                dict(
                    scope="account",
                    scope_key=uuid.uuid4(),
                    enabled=True
                )
            ]
        )

        result = api.update_feature_flag(
            objectview(feature_flag_enablement_input)
        )
        assert result['success']
        assert db.connection().execute(
            f"select count(*) from analytics.feature_flag_enablements where feature_flag_id='{feature_flag.id}'"
        ).scalar() == 2

class TestUpdateFeatureFlagStatus:

    def it_updates_feature_flag_status(self):
        name = 'Test feature flag'
        feature_flag = db.connection().execute(
            f"select * from analytics.feature_flags where name='{name}'"
        ).fetchone()

        update_feature_flag_status_input = dict(
            key=feature_flag.key,
            enable_all=True,
            active=True,
            enablements=None
        )

        result = api.update_feature_flag(objectview(update_feature_flag_status_input))

        assert result['success']
        enabled_status = db.connection().execute(
            f"select enable_all from analytics.feature_flags where key='{feature_flag.key}'"
        ).scalar()
        assert enabled_status == True


class TestDeactivateFeatureFlag:

    def it_deactivates_feature_flag(self):
        name = 'Test feature flag'
        feature_flag = db.connection().execute(
            f"select * from analytics.feature_flags where name='{name}'"
        ).fetchone()

        deactivate_feature_flag_input = dict(
            key=feature_flag.key,
            enable_all=False,
            active=False,
            enablements=None
        )

        result = api.update_feature_flag(objectview(deactivate_feature_flag_input))

        assert result['success']
        active_flag = db.connection().execute(
            f"select active from analytics.feature_flags where key='{feature_flag.key}'"
        ).scalar()
        assert active_flag == False
