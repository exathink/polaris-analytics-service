# -*- coding: utf-8 -*-

#  Copyright (c) Exathink, LLC  2016-2023.
#  All rights reserved
#

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene

from ..value_stream import ValueStream

class CreateValueStreamInput(graphene.InputObjectType):
    project_key = graphene.String(required=True)
    name = graphene.String (required=True)
    description = graphene.String(required=True)

    work_item_selectors = graphene.List(graphene.String, required=True)

class CreateValueStream(graphene.Mutation):
    class Arguments:
        create_value_stream_input = CreateValueStreamInput(required=True)

    value_stream = ValueStream.Field()
    success = graphene.Boolean()
    errorMessage = graphene.String()

    def mutate(self, info, create_value_stream_input):

        return CreateValueStream(
            success=False,
            value_stream=None
        )


class ValueStreamMutationsMixin:
    create_value_stream = CreateValueStream.Field()
