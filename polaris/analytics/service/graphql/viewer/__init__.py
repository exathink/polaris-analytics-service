
# Copyright: © Exathink, LLC (2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from flask_login import current_user

from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable

from ..account import Account
from ..interfaces import ScopedRole
from .selectables import ViewerAccountRoles, ViewerOrganizationRoles

from ..selectable_field_mixins import SelectablePropertyResolverMixin


class ScopedRoleField(graphene.ObjectType):
    class Meta:
        interfaces = (ScopedRole, )


class Viewer (
    SelectablePropertyResolverMixin,
    Selectable,
    graphene.ObjectType
):

    class Meta:
        interfaces = (NamedNode, )
        selectable_field_resolvers = {
            'account_roles': ViewerAccountRoles,
            'organization_roles': ViewerOrganizationRoles
        }


    # field definitions
    user_name = graphene.String()
    email = graphene.String()
    first_name = graphene.String()
    last_name = graphene.String()
    company = graphene.String()

    system_roles = graphene.Field(graphene.List(graphene.String))
    account_roles = graphene.Field(graphene.List(ScopedRoleField))
    organization_roles = graphene.Field(graphene.List(ScopedRoleField))

    account_key = graphene.String()
    account = Account.Field()

    def __init__(self, user):
        super().__init__(self)
        self.key = user.key
        self.name = f'{user.first_name} {user.last_name}'
        self.current_user = user


    @classmethod
    def Field(cls):
        return graphene.Field(cls)

    @classmethod
    def resolve_field(cls, info, **kwargs):
        return Viewer(current_user)

    def resolve_user_name(self, info, **kwargs):
        return self.current_user.user_name

    def resolve_email(self, info, **kwargs):
        return self.current_user.email

    def resolve_company(self, info, **kwargs):
        return self.current_user.company

    def resolve_first_name(self, info, **kwargs):
        return self.current_user.first_name

    def resolve_last_name(self, info, **kwargs):
        return self.current_user.last_name

    def resolve_system_roles(self, info, **kwargs):
        return [role.name for role in self.current_user.roles]

    def resolve_account_roles(self, info, **kwargs):
        return self.resolve_selectable_field('account_roles')

    def resolve_organization_roles(self, info, **kwargs):
        return self.resolve_selectable_field('organization_roles')

    def resolve_account_key(self, info, **kwargs):
        return self.current_user.account_key

    def resolve_account(self, info, **kwargs):
        return Account.resolve_field(info, **kwargs)