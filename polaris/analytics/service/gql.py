# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from functools import wraps
from flask import Blueprint, abort, request
from flask_graphql import GraphQLView
from flask_security import login_required
from flask_cors import cross_origin
from polaris.flask.common import admin_required

from polaris.analytics.service.graphql import schema


class Graphql(Blueprint):

    def register(self, app, options, first_registration=False):
        super().register(app, options, first_registration)

        # this is the default endpoint for client api.
        # in the dev mode it also brings up
        view = cross_origin(supports_credentials=True)(
            login_required(
                GraphQLView.as_view('graphql', schema=schema, graphiql=app.env == 'development')
            )
        )
        app.add_url_rule('/graphql/', view_func=view)

        # we also mount a graphiql that is
        # accessible only to admins.
        admin_iql = cross_origin(supports_credentials=True)(
            login_required(
                admin_required(
                    GraphQLView.as_view('admin_iql', schema=schema, graphiql=True)
                )
            )
        )
        app.add_url_rule('/graphql/i/', view_func=admin_iql)


graphql = Graphql('graphql', __name__)
