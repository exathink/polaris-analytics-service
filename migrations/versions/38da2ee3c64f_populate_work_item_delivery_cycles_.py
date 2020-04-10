"""populate_work_item_delivery_cycles_complexity_metrics

Revision ID: 38da2ee3c64f
Revises: dbbf66aca87d
Create Date: 2020-04-10 11:59:16.244603

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '38da2ee3c64f'
down_revision = 'dbbf66aca87d'
branch_labels = None
depends_on = None


def upgrade():
    # Update all delivery cycles with code change stats for merge and non merge commits
    op.execute("""
                WITH delivery_cycles_commits_rows AS
                 (SELECT analytics.work_item_delivery_cycles.delivery_cycle_id AS delivery_cycle_id,
                         SUM(CASE WHEN commits.num_parents = 1 THEN CAST(commits.stats->>'lines'as INTEGER) ELSE 0 END) as total_lines_changed,
           SUM(CASE WHEN commits.num_parents=1 THEN CAST(commits.stats->>'files'as INTEGER) ELSE 0 END) as total_files_changed,
           SUM(CASE WHEN commits.num_parents = 1 THEN CAST(commits.stats->>'deletions'as INTEGER) ELSE 0 END) as total_lines_deleted,
           SUM(CASE WHEN commits.num_parents = 1 THEN CAST(commits.stats->>'insertions'as INTEGER) ELSE 0 END) as total_lines_inserted,
           SUM(CASE WHEN commits.num_parents > 1 THEN CAST(commits.stats->>'lines'as INTEGER) ELSE 0 END) as total_lines_changed_merge,
            SUM(CASE WHEN commits.num_parents > 1 THEN CAST(commits.stats->>'files'as INTEGER) ELSE 0 END) as total_files_changed_merge,
            trunc(AVG(CASE WHEN commits.num_parents > 1 THEN CAST(commits.stats->>'lines'as INTEGER) ELSE 0 END)) as average_lines_changed_merge
                FROM analytics.work_items
                           JOIN analytics.work_item_delivery_cycles
                                ON analytics.work_item_delivery_cycles.work_item_id = analytics.work_items.id
                           JOIN analytics.work_items_commits
                                ON analytics.work_items_commits.work_item_id = analytics.work_items.id
                           JOIN analytics.commits ON analytics.work_items_commits.commit_id = analytics.commits.id
                WHERE analytics.commits.commit_date >= analytics.work_item_delivery_cycles.start_date
                GROUP BY analytics.work_item_delivery_cycles.delivery_cycle_id)
                UPDATE analytics.work_item_delivery_cycles
            SET total_lines_changed_non_merge = delivery_cycles_commits_rows.total_lines_changed,
                total_files_changed_non_merge = delivery_cycles_commits_rows.total_files_changed,
                total_lines_deleted_non_merge = delivery_cycles_commits_rows.total_lines_deleted,
                total_lines_inserted_non_merge = delivery_cycles_commits_rows.total_lines_inserted,
                total_lines_changed_merge = delivery_cycles_commits_rows.total_lines_changed_merge,
                total_files_changed_merge = delivery_cycles_commits_rows.total_files_changed_merge,
                average_lines_changed_merge = delivery_cycles_commits_rows.average_lines_changed_merge
            FROM delivery_cycles_commits_rows
            WHERE analytics.work_item_delivery_cycles.delivery_cycle_id = delivery_cycles_commits_rows.delivery_cycle_id
            """)


def downgrade():
    # Reset all complexity metrics values to null
    op.execute(
        "update analytics.work_item_delivery_cycles set total_lines_changed_non_merge = null, total_files_changed_non_merge = null , \
        total_lines_deleted_non_merge = null, total_lines_inserted_non_merge = null, total_lines_changed_merge = null, \
        total_files_changed_merge = null , average_lines_changed_merge = null ")
