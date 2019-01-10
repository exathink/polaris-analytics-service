# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select

from polaris.graphql.interfaces import NamedNode
from polaris.analytics.db.model import organizations, projects, repositories



class PublicOrganizationsNodes:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            organizations.c.id,
            organizations.c.key.label('key'),
            organizations.c.name
        ]).select_from(
            organizations
        ).where(organizations.c.public == True)


class PublicProjectsNodes:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            projects.c.id,
            projects.c.key.label('key'),
            projects.c.name
        ]).select_from(
            projects
        ).where(projects.c.public == True)


class PublicRepositoriesNodes:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            repositories.c.id,
            repositories.c.key,
            repositories.c.name
        ]).select_from(
            repositories
        ).where(repositories.c.public == True)


