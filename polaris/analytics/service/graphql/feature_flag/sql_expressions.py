# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar



def feature_flag_enablement_info_columns(feature_flag_enablements):
    return [
        feature_flag_enablements.c.enabled,
        feature_flag_enablements.c.scope_key,
        feature_flag_enablements.c.scope
    ]

