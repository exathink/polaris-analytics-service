# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import and_, or_, distinct, select, bindparam
from polaris.analytics.db.model import Contributor, ContributorAlias, Organization, Team, teams
from polaris.analytics.db.model import contributor_aliases, commits, repositories_contributor_aliases, \
    work_items, work_items_commits, work_items_teams
from polaris.utils.exceptions import ProcessingException
from sqlalchemy.dialects.postgresql import insert

def unlink_contributor_alias_from_contributor(session, contributor, contributor_alias_key):
    contributor_alias = ContributorAlias.find_by_contributor_alias_key(session, contributor_alias_key)
    original_contributor = Contributor.find_by_contributor_key(session, contributor_alias_key)
    if contributor_alias and original_contributor:
        # set the original contributor_id for this alias
        session.connection().execute(
            contributor_aliases.update().where(
                contributor_aliases.c.key == contributor_alias.key
            ).values(
                contributor_id=original_contributor.id
            )
        )
        # rewrite denormalized author_contributor info on commits referencing this alias
        session.connection().execute(
            commits.update().where(
                commits.c.author_contributor_alias_id == contributor_alias.id
            ).values(
                author_contributor_key=original_contributor.key,
                author_contributor_name=original_contributor.name
            )
        )
        # rewrite denormalized commiter_contributor info on commits referencing this alias
        session.connection().execute(
            commits.update().where(
                commits.c.committer_contributor_alias_id == contributor_alias.id
            ).values(
                committer_contributor_key=original_contributor.key,
                committer_contributor_name=original_contributor.name
            )
        )
        # rewrite the denormalized contributor info on repository_contributor aliases
        session.connection().execute(
            repositories_contributor_aliases.update().where(
                repositories_contributor_aliases.c.contributor_alias_id == contributor_alias.id
            ).values(
                contributor_id=original_contributor.id
            )
        )
    else:
        raise ProcessingException(f'Could not find contributor alias with key {contributor_alias_key}')


def update_contributor_for_contributor_alias(session, contributor, contributor_alias_key):
    contributor_alias = ContributorAlias.find_by_contributor_alias_key(session, contributor_alias_key)
    if contributor_alias:
        # set the new contributor_id for this alias
        session.connection().execute(
            contributor_aliases.update().where(
                contributor_aliases.c.id == contributor_alias.id
            ).values(
                contributor_id=contributor.id
            )
        )
        # rewrite denormalized author_contributor info on commits referencing this alias
        session.connection().execute(
            commits.update().where(
                commits.c.author_contributor_alias_id == contributor_alias.id
            ).values(
                author_contributor_key=contributor.key,
                author_contributor_name=contributor.name
            )
        )
        # rewrite denormalized commiter_contributor info on commits referencing this alias
        session.connection().execute(
            commits.update().where(
                commits.c.committer_contributor_alias_id == contributor_alias.id
            ).values(
                committer_contributor_key=contributor.key,
                committer_contributor_name=contributor.name
            )
        )

        # rewrite the denormalized contributor info on repository_contributor aliases
        session.connection().execute(
            repositories_contributor_aliases.update().where(
                repositories_contributor_aliases.c.contributor_alias_id == contributor_alias.id
            ).values(
                contributor_id=contributor.id
            )
        )
    else:
        raise ProcessingException(f'Could not find contributor alias with key {contributor_alias_key}')


def update_all_contributor_aliases(session, contributor, updated_fields):
    session.connection().execute(
        contributor_aliases.update().where(
            contributor_aliases.c.contributor_id == contributor.id
        ).values(
            updated_fields
        )
    )
    session.connection().execute(
        repositories_contributor_aliases.update().where(
            repositories_contributor_aliases.c.contributor_id ==contributor.id
        ).values(
            updated_fields
        )
    )


def update_contributor(session, contributor_key, updated_info):
    contributor = Contributor.find_by_contributor_key(session, contributor_key)
    if contributor:
        # Note: The order of processing these parameters is important. So should be preserved.
        if updated_info.get('contributor_name') is not None:
            contributor.update(dict(name=updated_info.get('contributor_name')))
        if updated_info.get('unlink_contributor_alias_keys'):
            for alias_key in updated_info.get('unlink_contributor_alias_keys'):
                unlink_contributor_alias_from_contributor(session, contributor, alias_key)
        if updated_info.get('contributor_alias_keys'):
            for alias_key in updated_info.get('contributor_alias_keys'):
                update_contributor_for_contributor_alias(session, contributor, alias_key)
        if updated_info.get('excluded_from_analysis') is not None:
            update_all_contributor_aliases(session,
                                           contributor,
                                           updated_fields=dict(
                                               robot=updated_info.get('excluded_from_analysis')
                                           ))
        return dict(
            updated_info=updated_info
        )
    else:
        raise ProcessingException(f"Contributor with key: {contributor_key} was not found")


def update_contributor_team_assignments(session, organization_key, contributor_team_assignments):
    organization = Organization.find_by_organization_key(session, organization_key)
    if organization is not None:
        updated_contributor_teams_assignments = []
        for assignment in contributor_team_assignments:
            contributor = Contributor.find_by_contributor_key(session, assignment.get('contributor_key'))
            if contributor is not None:
                initial_assignment = False
                target_team = organization.find_team(assignment.get('new_team_key'))
                if target_team is not None:
                    if len(contributor.teams) == 0:
                        initial_assignment = True
                    contributor.assign_to_team(session, target_team.key, assignment.get('capacity', 1.0))
                    updated_contributor_teams_assignments.append(
                        dict(
                            **assignment,
                            initial_assignment=initial_assignment
                        )
                    )
                else:
                    raise ProcessingException(f"Target team was not found in current organization for contributor {assignment.get('contributor_key')}")
            else:
                raise ProcessingException(f"Could not find contributor with key {assignment.get('contributor_key')}")

        return dict(
            update_count=len(contributor_team_assignments),
            updated_assignments=updated_contributor_teams_assignments
        )
    else:
        raise ProcessingException(f'No organization found for key: {organization_key}')


def assign_contributor_commits_to_teams(session, organization_key, contributor_team_assignments):
    assignment_count = 0
    for assignment in contributor_team_assignments:
        if assignment.get('initial_assignment'):
            contributor_key = assignment.get('contributor_key')
            team_key = assignment.get('new_team_key')
            team = Team.find_by_key(session, team_key)
            if team is not None:
                session.connection().execute(
                    commits.update().where(
                        and_(
                            commits.c.author_contributor_key == contributor_key,
                            teams.c.key == team_key
                        )
                    ).values(
                        author_team_key=team_key,
                        author_team_id=teams.c.id
                    )
                )

                session.connection().execute(
                    commits.update().where(
                        and_(
                            commits.c.committer_contributor_key == contributor_key,
                            teams.c.key == team_key
                        )
                    ).values(
                        committer_team_key=team_key,
                        committer_team_id=teams.c.id
                    )
                )

                # assign work items to teams based on the commits just associated
                assign_work_items_to_team(session, contributor_key, team_key)

                #
                assignment_count = assignment_count + 1

            else:
                raise ProcessingException(f'Could not find team with key {team_key} in organization {organization_key}')

    return dict(
        update_count=assignment_count
    )


def assign_work_items_to_team(session, contributor_key, team_key):
    # This assigns work items to teams by taking all
    # commits associated with  the given contributor as author or committer and
    # assigning all the work items associated with those commits to the team
    # specified. This is safe to use only when the team is initially assigned to the contributor
    team = Team.find_by_key(session, team_key)

    to_upsert = select([
        bindparam('team_id').label('team_id'),
        work_items_commits.c.work_item_id.label('work_item_id')
    ]).distinct().select_from(
        commits.join(
            work_items_commits, work_items_commits.c.commit_id == commits.c.id
        )
    ).where(
        or_(
            commits.c.author_team_key == team_key,
            commits.c.committer_team_key == team_key
        )
    )

    upsert = insert(work_items_teams).from_select(
        [
            to_upsert.c.team_id,
            to_upsert.c.work_item_id
        ],
        to_upsert
    )
    session.connection().execute(
        upsert.on_conflict_do_nothing(
            index_elements=['team_id', 'work_item_id'],
        ), dict(team_id=team.id)
    )
