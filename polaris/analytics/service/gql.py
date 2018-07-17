# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from flask import Blueprint
from flask_graphql import GraphQLView
from flask_security import login_required
from flask_cors import cross_origin

from polaris.analytics.service.graphql import schema

class Graphql(Blueprint):

    def register(self, app, options, first_registration=False):
        super().register(app, options, first_registration)
        gql_view = cross_origin(supports_credentials=True)(
            login_required(
                GraphQLView.as_view('graphql', schema=schema, graphiql=True)
            )
        )
        app.add_url_rule('/graphql/', view_func=gql_view)


graphql = Graphql('graphql', __name__)


