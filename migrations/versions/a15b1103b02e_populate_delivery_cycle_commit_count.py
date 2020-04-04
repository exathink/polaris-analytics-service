"""populate_delivery_cycle_commit_count

Revision ID: a15b1103b02e
Revises: 4028fa9bc42e
Create Date: 2020-04-04 09:21:45.552749

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a15b1103b02e'
down_revision = '4028fa9bc42e'
branch_labels = None
depends_on = None


def upgrade():
    # Update commit count
    op.execute("""
                WITH delivery_cycles_commits_rows AS
                 (SELECT analytics.work_item_delivery_cycles.delivery_cycle_id AS delivery_cycle_id,
                         count(distinct analytics.commits.id)                    AS commit_count
                  FROM analytics.work_items
                           JOIN analytics.work_item_delivery_cycles
                                ON analytics.work_item_delivery_cycles.work_item_id = analytics.work_items.id
                           JOIN analytics.work_items_commits
                                ON analytics.work_items_commits.work_item_id = analytics.work_items.id
                           JOIN analytics.commits ON analytics.work_items_commits.commit_id = analytics.commits.id
                  WHERE analytics.commits.commit_date >= analytics.work_item_delivery_cycles.start_date
                  GROUP BY analytics.work_item_delivery_cycles.delivery_cycle_id)
                UPDATE analytics.work_item_delivery_cycles
                SET commit_count=delivery_cycles_commits_rows.commit_count
                FROM delivery_cycles_commits_rows
                WHERE analytics.work_item_delivery_cycles.delivery_cycle_id = delivery_cycles_commits_rows.delivery_cycle_id
            """)


def downgrade():
    # Reset commit count
    op.execute("update analytics.work_item_delivery_cycles set commit_count = null")
