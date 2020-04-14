"""fix-work-item-commit_stats-data

Revision ID: a9d0d55502ea
Revises: 38da2ee3c64f
Create Date: 2020-04-13 23:05:19.719289

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a9d0d55502ea'
down_revision = '38da2ee3c64f'
branch_labels = None
depends_on = None


def upgrade():
    # Update all delivery cycles with commit_stats: earliest_commit, latest_commit, repository_count and commit_count
    op.execute("""
        WITH delivery_cycles_commits_rows AS
         (SELECT analytics.work_item_delivery_cycles.delivery_cycle_id AS delivery_cycle_id,
                 min(analytics.commits.commit_date)                    AS earliest_commit,
                 max(analytics.commits.commit_date)                    AS latest_commit,
                 count(analytics.commits.id)                           AS commit_count, 
                 count(distinct analytics.commits.repository_id)       AS repository_count
          FROM analytics.work_items
                   JOIN analytics.work_item_delivery_cycles
                        ON analytics.work_item_delivery_cycles.work_item_id = analytics.work_items.id
                   JOIN analytics.work_items_commits
                        ON analytics.work_items_commits.work_item_id = analytics.work_items.id
                   JOIN analytics.commits ON analytics.work_items_commits.commit_id = analytics.commits.id
          WHERE 
          analytics.commits.commit_date >= analytics.work_item_delivery_cycles.start_date 
          and (
            analytics.work_item_delivery_cycles.end_date is null or 
            analytics.commits.commit_date <= analytics.work_item_delivery_cycles.end_date
          )
          GROUP BY analytics.work_item_delivery_cycles.delivery_cycle_id)
        UPDATE analytics.work_item_delivery_cycles
    SET earliest_commit=delivery_cycles_commits_rows.earliest_commit,
    latest_commit=delivery_cycles_commits_rows.latest_commit,
    commit_count=delivery_cycles_commits_rows.commit_count,
    repository_count=delivery_cycles_commits_rows.repository_count
    FROM delivery_cycles_commits_rows
    WHERE analytics.work_item_delivery_cycles.delivery_cycle_id = delivery_cycles_commits_rows.delivery_cycle_id
    """)


def downgrade():
    # Reset the commit stats
    op.execute("update analytics.work_item_delivery_cycles "
               "set latest_commit = null, earliest_commit = null, repository_count = null, commit_count = null")

