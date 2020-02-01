"""add_work_items_state_source_map

Revision ID: 11506ba01d30
Revises: c74cb2b8db51
Create Date: 2020-01-22 09:49:35.484005

"""
from alembic import op
import sqlalchemy as sa
from polaris.analytics.db.enums import WorkItemsStateType
from polaris.common.enums import WorkTrackingIntegrationType
from sqlalchemy.ext.declarative import declarative_base

# revision identifiers, used by Alembic.
revision = '11506ba01d30'
down_revision = 'c74cb2b8db51'
branch_labels = None
depends_on = None

Base = declarative_base()

def get_default_state_map(integration_type):
    if integration_type == WorkTrackingIntegrationType.github.value:
        return [
            dict(state='created', state_type=WorkItemsStateType.open.value),
            dict(state='open', state_type=WorkItemsStateType.open.value),
            dict(state='closed', state_type=WorkItemsStateType.complete.value)
        ]
    elif integration_type == WorkTrackingIntegrationType.pivotal.value:
        return [
            dict(state='created', state_type=WorkItemsStateType.open.value),
            dict(state='unscheduled', state_type=WorkItemsStateType.open.value),
            dict(state='unstarted', state_type=WorkItemsStateType.open.value),
            dict(state='planned', state_type=WorkItemsStateType.open.value),
            dict(state='started', state_type=WorkItemsStateType.wip.value),
            dict(state='finished', state_type=WorkItemsStateType.wip.value),
            dict(state='delivered', state_type=WorkItemsStateType.wip.value),
            dict(state='accepted', state_type=WorkItemsStateType.complete.value)
        ]
    else:
        return []


def upgrade():

    work_items_source_state_map = op.create_table('work_items_source_state_map',
    sa.Column('state', sa.String(), nullable=False),
    sa.Column('state_type', sa.String(), server_default='open', nullable=False),
    sa.Column('work_items_source_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['work_items_source_id'], ['analytics.work_items_sources.id'], ),
    sa.PrimaryKeyConstraint('state', 'work_items_source_id'),
    schema='analytics'
    )

    work_items_sources = sa.Table(
        'work_items_sources', Base.metadata,
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('integration_type', sa.String()),
        schema='analytics'
    )

    conn = op.get_bind()
    session = sa.orm.Session(bind=conn)

    work_item_sources_data = session.execute(
            sa.select([
                work_items_sources.c.id,
                work_items_sources.c.integration_type
            ]).select_from(
                work_items_sources
            ).where(
                work_items_sources.c.integration_type.in_(('github', 'pivotal_tracker'))
            )
    )

    state_maps_data = []
    for work_item_source in work_item_sources_data:
        default_state_map = get_default_state_map(work_item_source.integration_type)
        state_maps_data.extend([dict(
            work_items_source_id=work_item_source.id,
            state=mapping["state"],
            state_type=mapping["state_type"]) for mapping in default_state_map])

    op.bulk_insert(work_items_source_state_map, state_maps_data)

def downgrade():
    op.drop_table('work_items_source_state_map', schema='analytics')
