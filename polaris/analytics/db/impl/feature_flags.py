# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert

from polaris.analytics.db.model import FeatureFlag, feature_flag_enablements, FeatureFlagEnablement
from polaris.utils.exceptions import ProcessingException

logger = logging.getLogger('polaris.analytics.db.impl')


def create_feature_flag(session, name):
    logger.info("Inside create_feature_flag")

    feature_flag = FeatureFlag.create(name=name)
    session.add(feature_flag)

    return dict(
        name=name,
        key=feature_flag.key
    )



def enable_feature_flag(session, feature_flag_key, enable_feature_flag_input):
    logger.info("Inside enable_feature_flag")
    feature_flag = FeatureFlag.find_by_key(session, feature_flag_key)
    if feature_flag is not None:
        enablements = insert(feature_flag_enablements).values([
            dict(
                feature_flag_id=feature_flag.id,
                **enablement
            )
            for enablement in enable_feature_flag_input
        ])
        inserted = session.connection().execute(
            enablements
        ).rowcount
        return dict(
            imported=inserted
        )
    else:
        raise ProcessingException(f"Could not find feature flag with key: {feature_flag_key}")

def update_enablements_status(session, feature_flag_key, update_enablements_status_input):
    logger.info("Inside update_enablements_status")
    feature_flag = FeatureFlag.find_by_key(session, feature_flag_key)
    updated = []
    if feature_flag is not None:
        for enablement in update_enablements_status_input:
            updated.append(session.execute(
                feature_flag_enablements.update().values(
                    enabled=enablement.enabled
                ).where(
                    and_(
                        feature_flag_enablements.c.scope_key == enablement.scope_key,
                        feature_flag_enablements.c.feature_flag_id == feature_flag.id
                    )
                )
            ))
        return dict(
            updated=updated
        )
    else:
        raise ProcessingException(f"Could not find feature flag with key: {feature_flag_key}")