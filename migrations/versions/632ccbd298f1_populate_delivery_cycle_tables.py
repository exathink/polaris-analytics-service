"""populate_delivery_cycle_tables

Revision ID: 632ccbd298f1
Revises: c80b3cc4f7ee
Create Date: 2020-03-15 15:26:29.169648

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '632ccbd298f1'
down_revision = 'c80b3cc4f7ee'
branch_labels = None
depends_on = None


def upgrade():
    # Insert the initial delivery cycles for each work item
    op.execute("""
        insert into analytics.work_item_delivery_cycles
            (work_item_id, start_seq_no, start_date)
        select
            work_item_id,
            seq_no as start_seq_no,
            created_at as start_date
        from
            analytics.work_item_state_transitions where seq_no=0
    """)

    # Add additional delivery cycles for work_items that were re-opened
    op.execute("""
        insert into analytics.work_item_delivery_cycles
            (work_item_id, start_seq_no, start_date)
        select
            work_item_id,
            seq_no as start_seq_no,
            created_at as start_date
        from
            analytics.work_item_state_transitions where previous_state in ('closed', 'accepted')
    """)

    # Compute the end date and lead time for delivery cycles that have closed.
    op.execute("""
        update analytics.work_item_delivery_cycles
        set end_date=analytics.work_item_state_transitions.created_at,
            lead_time=extract(epoch from analytics.work_item_state_transitions.created_at) - extract(epoch from analytics.work_item_delivery_cycles.start_date)
        from analytics.work_item_delivery_cycles as wid, analytics.work_item_state_transitions
        where
              work_item_delivery_cycles.work_item_id = work_item_state_transitions.work_item_id and
              work_item_state_transitions.state in ('accepted', 'closed')  and
              wid.start_date < work_item_state_transitions.created_at
    """)

    # Populate the current_delivery_cycle_id for each work_item
    op.execute("""
        With latest_delivery_cycle as (
            Select work_item_id, max(delivery_cycle_id) as delivery_cycle_id from
            analytics.work_item_delivery_cycles
            group by work_item_id
        )
        update analytics.work_items
            set current_delivery_cycle_id = latest_delivery_cycle.delivery_cycle_id
        from latest_delivery_cycle
        where latest_delivery_cycle.work_item_id = work_items.id
    """)


def downgrade():
    # Reset current delivery cycle id
    op.execute("update analytics.work_items set current_delivery_cycle_id = null");

    # clear out delivery cycles
    op.execute("delete from analytics.work_item_delivery_cycles")
