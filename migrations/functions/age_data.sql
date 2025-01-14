/*
 * Copyright (c) Exathink, LLC  2016-2025.
 * All rights reserved
 *
 */

CREATE OR REPLACE PROCEDURE age_data(org_key UUID)
LANGUAGE plpgsql
AS $$
DECLARE
    offset_days INTERVAL;
BEGIN
    -- Calculate the offset
    SELECT min(days_since_latest_update) * interval '1 day'
    INTO offset_days
    FROM (
        SELECT min(now()::date - updated_at::date) AS days_since_latest_update
        FROM analytics.work_items
                 INNER JOIN analytics.work_items_sources
                            ON work_items.work_items_source_id = work_items_sources.id
        WHERE work_items_sources.organization_key = org_key

        UNION ALL

        SELECT min(now()::date - latest_commit::date) AS days_since_latest_update
        FROM analytics.repositories
                 INNER JOIN analytics.organizations
                            ON repositories.organization_id = organizations.id
        WHERE organizations.key = org_key

        UNION ALL

        SELECT min(now()::date - updated_at::date) AS days_since_latest_update
        FROM analytics.pull_requests
                 INNER JOIN analytics.repositories
                            ON pull_requests.repository_id = repositories.id
                 INNER JOIN analytics.organizations
                            ON repositories.organization_id = organizations.id
        WHERE organizations.key = org_key
    ) AS aging_offset;

    -- Update all relevant tables with the calculated offset
    UPDATE analytics.work_items
    SET created_at = created_at + offset_days,
        updated_at = updated_at + offset_days,
        completed_at = completed_at + offset_days
    WHERE id IN (
        SELECT wi.id
        FROM analytics.work_items wi
                 INNER JOIN analytics.work_items_sources
                            ON wi.work_items_source_id = work_items_sources.id
        WHERE work_items_sources.organization_key = org_key
    );

    UPDATE analytics.work_item_state_transitions
    SET created_at = created_at + offset_days
    WHERE work_item_id IN (
        SELECT wi.id
        FROM analytics.work_items wi
                 INNER JOIN analytics.work_items_sources
                            ON wi.work_items_source_id = work_items_sources.id
        WHERE work_items_sources.organization_key = org_key
    );

    UPDATE analytics.work_item_delivery_cycles
    SET start_date = start_date + offset_days,
        end_date = end_date + offset_days,
        earliest_commit = earliest_commit + offset_days,
        latest_commit = latest_commit + offset_days
    WHERE work_item_id IN (
        SELECT wi.id
        FROM analytics.work_items wi
                 INNER JOIN analytics.work_items_sources
                            ON wi.work_items_source_id = work_items_sources.id
        WHERE work_items_sources.organization_key = org_key
    );

    UPDATE analytics.pull_requests
    SET created_at = created_at + offset_days,
        updated_at = updated_at + offset_days,
        deleted_at = deleted_at + offset_days,
        end_date = end_date + offset_days
    WHERE repository_id IN (
        SELECT repositories.id
        FROM analytics.repositories
                 INNER JOIN analytics.organizations
                            ON repositories.organization_id = organizations.id
        WHERE organizations.key = org_key
    );

    UPDATE analytics.commits
    SET created_at = created_at + offset_days,
        commit_date = commit_date + offset_days,
        author_date = author_date + offset_days
    WHERE repository_id IN (
        SELECT repositories.id
        FROM analytics.repositories
                 INNER JOIN analytics.organizations
                            ON repositories.organization_id = organizations.id
        WHERE organizations.key = org_key
    );

    UPDATE analytics.repositories
    SET earliest_commit = earliest_commit + offset_days,
        latest_commit = latest_commit + offset_days
    WHERE id IN (
        SELECT repositories.id
        FROM analytics.repositories
                 INNER JOIN analytics.organizations
                            ON repositories.organization_id = organizations.id
        WHERE organizations.key = org_key
    );

    UPDATE analytics.repositories_contributor_aliases
    SET earliest_commit = earliest_commit + offset_days,
        latest_commit = latest_commit + offset_days
    WHERE repository_id IN (
        SELECT repositories.id
        FROM analytics.repositories
                 INNER JOIN analytics.organizations
                            ON repositories.organization_id = organizations.id
        WHERE organizations.key = org_key
    );

    UPDATE analytics.teams_repositories
    SET earliest_commit = earliest_commit + offset_days,
        latest_commit = latest_commit + offset_days
    WHERE repository_id IN (
        SELECT repositories.id
        FROM analytics.repositories
                 INNER JOIN analytics.organizations
                            ON repositories.organization_id = organizations.id
        WHERE organizations.key = org_key
    );
END;
$$;
