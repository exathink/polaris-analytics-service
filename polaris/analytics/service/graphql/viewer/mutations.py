# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
import logging
from polaris.analytics import api
from flask_login import current_user
from polaris.common import db
from polaris.analytics.db.utils import find_or_create_account_for_user
from polaris.utils.exceptions import ProcessingException
from ..viewer import Viewer

logger = logging.getLogger('polaris.analytics.graphql')


class InitViewerAccountInput(graphene.InputObjectType):
    account_name = graphene.String(required=False)
    organization_name = graphene.String(required=True)


class InitViewerAccount(graphene.Mutation):
    class Arguments:
        init_viewer_account_input = InitViewerAccountInput(required=True)

    viewer = Viewer.Field()

    def mutate(self, info, init_viewer_account_input):
        logger.info('InitViewer Account called')
        account = api.init_viewer_account(
            account_name=init_viewer_account_input.account_name,
            organization_name=init_viewer_account_input.organization_name
        )
        if account is not None:
            return InitViewerAccount(
                    viewer=Viewer.resolve_field(info)
            )

        else:
            raise ProcessingException("Account was not created")


class ViewerMutationsMixin:
    init_viewer_account = InitViewerAccount.Field()