"""populate_work_item_delivery_cycle_time

Revision ID: b5af1576746c
Revises: 9e049da8b739
Create Date: 2020-04-20 15:38:54.603996

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b5af1576746c'
down_revision = '9e049da8b739'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
                WITH delivery_cycle_cycle_time AS
             (SELECT analytics.work_item_delivery_cycles.delivery_cycle_id AS delivery_cycle_id,
                     SUM(CASE
                             WHEN work_items_source_state_map.state_type IN ('open', 'wip', 'complete')
                                 THEN work_item_delivery_cycle_durations.cumulative_time_in_state
                             ELSE NULL END)                                as cycle_time
                FROM analytics.work_item_delivery_cycles
                JOIN analytics.work_item_delivery_cycle_durations
                    ON analytics.work_item_delivery_cycles.delivery_cycle_id =
                       analytics.work_item_delivery_cycle_durations.delivery_cycle_id
                full outer join analytics.work_items
                               ON analytics.work_item_delivery_cycles.work_item_id =
                                  analytics.work_items.id
                JOIN analytics.work_items_source_state_map
                    ON analytics.work_items.work_items_source_id =
                               analytics.work_items_source_state_map.work_items_source_id
                WHERE analytics.work_item_delivery_cycle_durations.state = analytics.work_items_source_state_map.state
                and analytics.work_item_delivery_cycles.end_date is not NULL
                GROUP BY work_item_delivery_cycles.delivery_cycle_id)
                UPDATE analytics.work_item_delivery_cycles
                SET cycle_time = delivery_cycle_cycle_time.cycle_time
                FROM delivery_cycle_cycle_time
                WHERE analytics.work_item_delivery_cycles.delivery_cycle_id = delivery_cycle_cycle_time.delivery_cycle_id
                and end_date is not NULL""")


def downgrade():
    op.execute("update analytics.work_item_delivery_cycles set cycle_time = NULL")
