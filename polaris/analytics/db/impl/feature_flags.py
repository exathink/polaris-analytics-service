# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert

from polaris.analytics.db.model import FeatureFlag, feature_flag_enablements
from polaris.utils.exceptions import ProcessingException

logger = logging.getLogger('polaris.analytics.db.impl')


def create_feature_flag(session, name):
    logger.info("Inside create_feature_flag")
    try:
        feature_flag = FeatureFlag.create(name=name)
        session.add(feature_flag)
    except IntegrityError:
        raise ProcessingException(f'Feature Flag {name} already exists')
    return dict(
        feature_flag=feature_flag,
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

