# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy.dialects.postgresql import insert
from polaris.analytics.db.model import repositories, Organization
from polaris.utils.exceptions import ProcessingException


def import_repositories(session, organization_key, repository_summaries):
    organization = Organization.find_by_organization_key(session, organization_key)
    if organization is not None:
        upsert = insert(repositories).values([
                dict(

                    organization_id=organization.id,
                    **repository_summary
                )
                for repository_summary in repository_summaries
            ])
        inserted = session.connection().execute(
            upsert.on_conflict_do_update(
                index_elements=['key'],
                set_=dict(
                    name=upsert.excluded.name,
                    description=upsert.excluded.description,
                    url=upsert.excluded.url,
                    public=upsert.excluded.public,
                )
            )
        ).rowcount
        return dict(
            imported=inserted
        )

    else:
        raise ProcessingException(f"Could not find organization with key: {organization_key}")
