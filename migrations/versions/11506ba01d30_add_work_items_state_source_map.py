"""add_work_items_state_source_map

Revision ID: 11506ba01d30
Revises: c74cb2b8db51
Create Date: 2020-01-22 09:49:35.484005

"""
from alembic import op
import sqlalchemy as sa
from polaris.analytics.db.enums import WorkItemsStateType
from polaris.common import db

# revision identifiers, used by Alembic.
revision = '11506ba01d30'
down_revision = 'c74cb2b8db51'
branch_labels = None
depends_on = None


def upgrade():

    work_items_source_state_map = op.create_table('work_items_source_state_map',
    sa.Column('state', sa.String(), nullable=False),
    sa.Column('state_type', sa.String(), server_default='open', nullable=False),
    sa.Column('work_items_source_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['work_items_source_id'], ['analytics.work_items_sources.id'], ),
    sa.PrimaryKeyConstraint('state', 'work_items_source_id'),
    schema='analytics'
    )

    state_mapping = {'created': WorkItemsStateType.open.value, 'open': WorkItemsStateType.open.value, 'closed': WorkItemsStateType.complete.value,
               'unscheduled': WorkItemsStateType.open.value, 'unstarted': WorkItemsStateType.open.value, 'planned': WorkItemsStateType.open.value,
               'started': WorkItemsStateType.open.value,
               'finished': WorkItemsStateType.wip.value, 'delivered': WorkItemsStateType.wip.value, 'accepted': WorkItemsStateType.complete.value}

    conn = op.get_bind()

    work_item_sources_data = conn.execute("SELECT DISTINCT work_item_state_transitions.state,  work_items_source_id from analytics.work_items_sources "
               "INNER JOIN analytics.work_items on analytics.work_items.work_items_source_id = analytics.work_items_sources.id "
               "INNER JOIN analytics.work_item_state_transitions ON analytics.work_items.id =  analytics.work_item_state_transitions.work_item_id  "
               "WHERE integration_type IN ('github', 'pivotal_tracker')")

    results = work_item_sources_data.fetchall()

    work_items_source_state_map_entries = [
        {'state': str(row[0]), 'state_type': state_mapping[str(row[0])], 'work_items_source_id': str(row[1])} for row in results]
    op.bulk_insert(work_items_source_state_map, work_items_source_state_map_entries)

def downgrade():
    op.drop_table('work_items_source_state_map', schema='analytics')
