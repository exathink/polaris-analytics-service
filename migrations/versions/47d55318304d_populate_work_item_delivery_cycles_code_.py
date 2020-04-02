"""populate_work_item_delivery_cycles_code_change_stats

Revision ID: 47d55318304d
Revises: 13fe1b26ccb9
Create Date: 2020-04-02 16:40:46.769852

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '47d55318304d'
down_revision = '13fe1b26ccb9'
branch_labels = None
depends_on = None


def upgrade():
    # Update all delivery cycles with code change stats:
    # total_lines_changed, total_files_changed, total_lines_deleted, total_lines_inserted
    op.execute("""
            WITH delivery_cycles_commits_rows AS
             (SELECT analytics.work_item_delivery_cycles.delivery_cycle_id AS delivery_cycle_id,
                     SUM(CASE WHEN commits.num_parents = 1 THEN CAST(commits.stats->>'lines'as INTEGER) ELSE 0 END) as total_lines_changed,
       SUM(CASE WHEN commits.num_parents=1 THEN CAST(commits.stats->>'files'as INTEGER) ELSE 0 END) as total_files_changed,
       SUM(CASE WHEN commits.num_parents = 1 THEN CAST(commits.stats->>'deletions'as INTEGER) ELSE 0 END) as total_lines_deleted,
       SUM(CASE WHEN commits.num_parents = 1 THEN CAST(commits.stats->>'insertions'as INTEGER) ELSE 0 END) as total_lines_inserted
            FROM analytics.work_items
                       JOIN analytics.work_item_delivery_cycles
                            ON analytics.work_item_delivery_cycles.work_item_id = analytics.work_items.id
                       JOIN analytics.work_items_commits
                            ON analytics.work_items_commits.work_item_id = analytics.work_items.id
                       JOIN analytics.commits ON analytics.work_items_commits.commit_id = analytics.commits.id
            WHERE analytics.commits.commit_date >= analytics.work_item_delivery_cycles.start_date
            GROUP BY analytics.work_item_delivery_cycles.delivery_cycle_id)
            UPDATE analytics.work_item_delivery_cycles
        SET total_lines_changed = delivery_cycles_commits_rows.total_lines_changed,
        total_files_changed = delivery_cycles_commits_rows.total_files_changed,
        total_lines_deleted = delivery_cycles_commits_rows.total_lines_deleted,
        total_lines_inserted = delivery_cycles_commits_rows.total_lines_inserted
        FROM delivery_cycles_commits_rows
        WHERE analytics.work_item_delivery_cycles.delivery_cycle_id = delivery_cycles_commits_rows.delivery_cycle_id
        """)


def downgrade():
    # Reset total_lines_changed, total_files_changed, total_lines_deleted, total_lines_inserted to null
    op.execute(
        "update analytics.work_item_delivery_cycles set total_lines_changed = null, total_files_changed = null , \
        total_lines_deleted = null, total_lines_inserted = null ")
