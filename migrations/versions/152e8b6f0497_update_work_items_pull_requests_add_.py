"""update_work_items_pull_requests_add_column_delivery_cycle_id

Revision ID: 152e8b6f0497
Revises: e96f191fca2e
Create Date: 2020-10-01 11:49:40.486599

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '152e8b6f0497'
down_revision = 'e96f191fca2e'
branch_labels = None
depends_on = None


def update_delivery_cycle_id():
    op.execute("""
                with work_items_pull_requests_delivery_cycles as (
                    select max(work_item_delivery_cycles.delivery_cycle_id) as delivery_cycle_id,
                    pull_requests.id                                 as pull_request_id,
                    work_items.id                                    as work_item_id
                    from analytics.work_items_pull_requests
                    inner join
                    analytics.pull_requests on work_items_pull_requests.pull_request_id = pull_requests.id
                    inner join analytics.work_items on work_items_pull_requests.work_item_id = work_items.id
                    inner join analytics.work_item_delivery_cycles on work_items.id = work_item_delivery_cycles.work_item_id
                    where start_date >= work_items.created_at
                    group by work_items.id, pull_requests.id)
                update analytics.work_items_pull_requests
                set delivery_cycle_id = work_items_pull_requests_delivery_cycles.delivery_cycle_id
                from work_items_pull_requests_delivery_cycles
                where work_items_pull_requests.work_item_id = work_items_pull_requests_delivery_cycles.work_item_id
                and work_items_pull_requests.pull_request_id = work_items_pull_requests_delivery_cycles.pull_request_id;
                """)

    op.execute("""
                update analytics.work_items_pull_requests
                set delivery_cycle_id=work_items.current_delivery_cycle_id
                from analytics.work_items
                where work_items_pull_requests.work_item_id = work_items.id
                  and work_items_pull_requests.delivery_cycle_id is NULL
                """)


def upgrade():
    op.add_column('work_items_pull_requests', sa.Column('delivery_cycle_id', sa.Integer(), nullable=True),
                  schema='analytics')
    op.create_index(op.f('ix_analytics_work_items_pull_requests_delivery_cycle_id'), 'work_items_pull_requests',
                    ['delivery_cycle_id'], unique=False, schema='analytics')
    op.create_foreign_key(None, 'work_items_pull_requests', 'work_item_delivery_cycles', ['delivery_cycle_id'],
                          ['delivery_cycle_id'], source_schema='analytics', referent_schema='analytics',
                          ondelete='SET NULL')
    update_delivery_cycle_id()


def downgrade():
    op.drop_constraint(None, 'work_items_pull_requests', schema='analytics', type_='foreignkey')
    op.drop_index(op.f('ix_analytics_work_items_pull_requests_delivery_cycle_id'),
                  table_name='work_items_pull_requests', schema='analytics')
    op.drop_column('work_items_pull_requests', 'delivery_cycle_id', schema='analytics')
