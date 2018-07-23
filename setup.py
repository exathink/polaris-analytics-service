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
        'polaris.analytics.cli',
        'polaris.analytics.service',
        'polaris.analytics.datasources',
        'polaris.analytics.datasources.activities',
	'polaris.analytics.service.graphql',
	'polaris.analytics.service.graphql.account', 
	'polaris.analytics.service.graphql.organization', 
	'polaris.analytics.service.graphql.project',
    'polaris.analytics.service.graphql.repository'
    ],

    url='',
    license = 'Commercial',
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
