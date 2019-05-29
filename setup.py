# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from setuptools import setup
from os import path

import polaris.analytics.service

here = path.abspath(path.dirname(__file__))

setup(
    # --------------------------------------------------------------------------------

    name='polaris.analytics.service',

    # -------------------------------------------------------------------------------

    version=polaris.analytics.service.__version__,

    # -------------------------------------------------------------------------------

    packages=[
        'polaris',
        'polaris.analytics',
        'polaris.analytics.api',
        'polaris.analytics.cli',
        'polaris.analytics.messaging',
        'polaris.analytics.messaging.commands',
        'polaris.analytics.messaging.subscribers',
        'polaris.analytics.service',
        'polaris.analytics.db',
        'polaris.analytics.db.impl',
        'polaris.analytics.datasources',
        'polaris.analytics.datasources.activities',
        'polaris.analytics.service.graphql',
        'polaris.analytics.service.graphql.viewer',
        'polaris.analytics.service.graphql.user',
        'polaris.analytics.service.graphql.account',
        'polaris.analytics.service.graphql.commit',
        'polaris.analytics.service.graphql.organization',
        'polaris.analytics.service.graphql.project',
        'polaris.analytics.service.graphql.repository',
        'polaris.analytics.service.graphql.public',
        'polaris.analytics.service.graphql.contributor',
        'polaris.analytics.service.graphql.work_item',
        'polaris.analytics.service.graphql.work_items_source',
        'polaris.analytics.service.graphql.summarizers',

    ],
    include_package_data=True,
    url='',
    license='Commercial',
    author='Krishna Kumar',
    author_email='kkumar@exathink.com',
    description='',
    long_description='',
    classifiers=[
        'Programming Language :: Python :: 3.5'
    ],
    # Run time dependencies - we will assume pytest is dependency of all packages.
    install_requires=[
        'pytest'
    ]
)
