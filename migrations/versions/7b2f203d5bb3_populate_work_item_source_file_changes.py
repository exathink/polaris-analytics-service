"""populate_work_item_source_file_changes

Revision ID: 7b2f203d5bb3
Revises: 5afd72f9d707
Create Date: 2020-04-29 02:43:38.735374

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '7b2f203d5bb3'
down_revision = '5afd72f9d707'
branch_labels = None
depends_on = None


def upgrade():
    # insert into work_item_source_file_changes
    op.execute("""
        WITH source_files_details AS
         (SELECT work_items.id                as                 work_item_id,
                 (CASE
                        WHEN (commits.commit_date >= work_item_delivery_cycles.start_date 
                        AND commits.commit_date <= work_item_delivery_cycles.end_date)
                        THEN work_item_delivery_cycles.delivery_cycle_id
                        ELSE NULL END) as delivery_cycle_id,
                 work_items_commits.commit_id as                 commit_id,
                 source_files.id              as                 source_file_id,
                 analytics.source_files.repository_id,
                 commit_date,
                 source_commit_id,
                 committer_contributor_alias_id,
                 author_contributor_alias_id,
                 num_parents,
                 created_on_branch,
                 sf ->> 'action' as                              file_action,
                 CAST(sf -> 'stats' ->> 'lines' AS INTEGER) as     total_lines_changed,
                 CAST(sf -> 'stats' ->> 'deletions' As INTEGER) as total_lines_deleted,
                 CAST(sf -> 'stats' ->> 'insertions' AS INTEGER) as total_lines_added
          FROM analytics.source_files,
               analytics.work_items,
               analytics.commits,
               analytics.work_items_commits,
               analytics.work_item_delivery_cycles,
               jsonb_array_elements(analytics.commits.source_files) sf
          where UUID(sf ->> 'key') = source_files.key
            AND work_items.id = work_items_commits.work_item_id
            AND work_items_commits.commit_id = commits.id
            AND work_items.id = work_item_delivery_cycles.work_item_id
            AND commits.num_parents = 1)
        INSERT
        INTO analytics.work_item_source_file_changes(work_item_id, delivery_cycle_id, repository_id, source_file_id, commit_id,
                                             source_commit_id, commit_date, committer_contributor_alias_id,
                                             author_contributor_alias_id, num_parents, created_on_branch, file_action,
                                             total_lines_changed, total_lines_deleted, total_lines_added)
        SELECT work_item_id,
            delivery_cycle_id,
            repository_id,
            source_file_id,
            commit_id,
            source_commit_id,
            commit_date,
            committer_contributor_alias_id,
            author_contributor_alias_id,
            num_parents,
            created_on_branch,
            file_action,
            total_lines_changed,
            total_lines_deleted,
            total_lines_added
        from source_files_details""")


def downgrade():
    op.execute("delete from analytics.work_item_source_file_changes")
