"""populate_work_item_delivery_cycles_commits_span

Revision ID: 60f21767bab9
Revises: f324bcbf49cc
Create Date: 2020-03-31 10:24:54.993639

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '60f21767bab9'
down_revision = 'f324bcbf49cc'
branch_labels = None
depends_on = None


def upgrade():
    # Update all delivery cycles with earliest commit and latest commit
    op.execute("""
        WITH delivery_cycles_commits_rows AS
         (SELECT analytics.work_item_delivery_cycles.delivery_cycle_id AS delivery_cycle_id,
                 min(analytics.commits.commit_date)                    AS earliest_commit,
                 max(analytics.commits.commit_date)                    AS latest_commit
          FROM analytics.work_items
                   JOIN analytics.work_item_delivery_cycles
                        ON analytics.work_item_delivery_cycles.work_item_id = analytics.work_items.id
                   JOIN analytics.work_items_commits
                        ON analytics.work_items_commits.work_item_id = analytics.work_items.id
                   JOIN analytics.commits ON analytics.work_items_commits.commit_id = analytics.commits.id
          WHERE analytics.commits.commit_date >= analytics.work_item_delivery_cycles.start_date
          GROUP BY analytics.work_item_delivery_cycles.delivery_cycle_id)
        UPDATE analytics.work_item_delivery_cycles
    SET earliest_commit=delivery_cycles_commits_rows.earliest_commit,
    latest_commit=delivery_cycles_commits_rows.latest_commit
    FROM delivery_cycles_commits_rows
    WHERE analytics.work_item_delivery_cycles.delivery_cycle_id = delivery_cycles_commits_rows.delivery_cycle_id
    """)


def downgrade():
    # Reset earliest commit and latest commit
    op.execute("update analytics.work_item_delivery_cycles set latest_commit = null, earliest_commit = null")
