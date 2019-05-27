# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, bindparam

from ..interfaces import ScopedRole
from polaris.analytics.db.model import \
    accounts, account_members, \
    organizations, organization_members


class ViewerAccountRoles:
    interfaces = (ScopedRole, )

    @staticmethod
    def selectable(**kwargs):
        return select([
            accounts.c.key.label('scope_key'),
            account_members.c.role,
        ]).select_from(
            accounts.join(account_members, accounts.c.id == account_members.c.account_id)
        ).where(account_members.c.user_key == bindparam('key'))


class ViewerOrganizationRoles:
    interfaces = (ScopedRole, )

    @staticmethod
    def selectable(**kwargs):
        return select([
            organizations.c.key.label('scope_key'),
            organization_members.c.role,
        ]).select_from(
            organizations.join(organization_members, organizations.c.id == organization_members.c.organization_id)
        ).where(organization_members.c.user_key == bindparam('key'))