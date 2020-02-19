# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from polaris.analytics.db.model import FeatureFlag
from polaris.utils.exceptions import ProcessingException

logger = logging.getLogger('polaris.analytics.db.impl')


def create_feature_flag(session, create_feature_flag_input):
    logger.info("Inside create_feature_flag")
    if create_feature_flag_input.name:
        feature_flag = FeatureFlag.create(name=create_feature_flag_input.name)
    else:
        raise ProcessingException(f'Could not create a new feature flag')
    session.add(feature_flag)
    return dict(
        feature_flag_key=feature_flag.key,
        feature_flag_name=feature_flag.name
    )

