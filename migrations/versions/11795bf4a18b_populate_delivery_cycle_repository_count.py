"""populate_delivery_cycle_repository_count

Revision ID: 11795bf4a18b
Revises: 89d03a08133f
Create Date: 2020-03-31 12:59:58.550770

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '11795bf4a18b'
down_revision = '89d03a08133f'
branch_labels = None
depends_on = None


def upgrade():
    # Update repository count
    op.execute("""
        WITH delivery_cycles_commits_rows AS
         (SELECT analytics.work_item_delivery_cycles.delivery_cycle_id AS delivery_cycle_id,
                 count(distinct analytics.commits.repository_id)                    AS repository_count
          FROM analytics.work_items
                   JOIN analytics.work_item_delivery_cycles
                        ON analytics.work_item_delivery_cycles.work_item_id = analytics.work_items.id
                   JOIN analytics.work_items_commits
                        ON analytics.work_items_commits.work_item_id = analytics.work_items.id
                   JOIN analytics.commits ON analytics.work_items_commits.commit_id = analytics.commits.id
          WHERE analytics.commits.commit_date >= analytics.work_item_delivery_cycles.start_date
          GROUP BY analytics.work_item_delivery_cycles.delivery_cycle_id)
        UPDATE analytics.work_item_delivery_cycles
        SET repository_count=delivery_cycles_commits_rows.repository_count
        FROM delivery_cycles_commits_rows
        WHERE analytics.work_item_delivery_cycles.delivery_cycle_id = delivery_cycles_commits_rows.delivery_cycle_id
    """)


def downgrade():
    # Reset repository count
    op.execute("update analytics.work_item_delivery_cycles set repository_count = null")
