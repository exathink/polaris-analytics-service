"""populate_delivery_cycle_durations_table

Revision ID: af1817e282a8
Revises: 632ccbd298f1
Create Date: 2020-03-19 11:04:47.189769

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'af1817e282a8'
down_revision = '632ccbd298f1'
branch_labels = None
depends_on = None


def upgrade():
    # Insert the initial delivery_cycle_durations
    op.execute("""
    insert into analytics.work_item_delivery_cycle_durations (state, cumulative_time_in_state, delivery_cycle_id)
        select
                state,
                sum(duration) as cumulative_time_in_state,
                delivery_cycle_id
        from (
                 select work_item_id,
                        delivery_cycle_id,
                        state,
                        created_at,
                        next_created_at,
                        extract(epoch from (next_created_at - created_at)) as duration
                 from (
                          select work_item_delivery_cycles.delivery_cycle_id,
                                 work_item_state_transitions.state,
                                 work_item_state_transitions.created_at,
                                 work_item_delivery_cycles.work_item_id,
                                 seq_no,
                                 LEAD(work_item_state_transitions.created_at) over (
                                     partition by work_item_state_transitions.work_item_id
                                     order by seq_no
                                     ) as next_created_at
                          from analytics.work_item_delivery_cycles
                                inner join analytics.work_item_state_transitions
                                          on work_item_delivery_cycles.work_item_id = work_item_state_transitions.work_item_id
                      ) as foo
             ) as bar
        group by delivery_cycle_id, work_item_id, state
        order by work_item_id, state
    """)


def downgrade():
    # clear out delivery cycle durations
    op.execute("delete from analytics.work_item_delivery_cycle_durations")
