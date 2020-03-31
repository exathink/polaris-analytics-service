"""populate_delivery_cycle_commit_count

Revision ID: 9f3835689a21
Revises: d3f7c0bee31f
Create Date: 2020-03-31 17:42:56.376582

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9f3835689a21'
down_revision = 'd3f7c0bee31f'
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
