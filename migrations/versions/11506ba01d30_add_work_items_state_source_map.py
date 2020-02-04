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


def insert_default_state_maps():
    #  github

    op.execute(f"""
            insert into analytics.work_items_source_state_map (state, state_type, work_items_source_id)
            select 'created', '{WorkItemsStateType.open.value}', 
            id from analytics.work_items_sources where integration_type='{WorkTrackingIntegrationType.github.value}';
        """)
    op.execute(f"""
            insert into analytics.work_items_source_state_map (state, state_type, work_items_source_id)
            select 'open', '{WorkItemsStateType.open.value}', 
            id from analytics.work_items_sources where integration_type='{WorkTrackingIntegrationType.github.value}';
        """)
    op.execute(f"""
            insert into analytics.work_items_source_state_map (state, state_type, work_items_source_id)
            select 'closed', '{WorkItemsStateType.complete.value}', 
            id from analytics.work_items_sources where integration_type='{WorkTrackingIntegrationType.github.value}';
        """)

    # {WorkTrackingIntegrationType.pivotal.value}
    op.execute(f"""
            insert into analytics.work_items_source_state_map (state, state_type, work_items_source_id)
            select 'created', '{WorkItemsStateType.open.value}', 
            id from analytics.work_items_sources where integration_type='{WorkTrackingIntegrationType.pivotal.value}';
        """)
    op.execute(f"""
            insert into analytics.work_items_source_state_map (state, state_type, work_items_source_id)
            select 'unscheduled', '{WorkItemsStateType.open.value}', 
            id from analytics.work_items_sources where integration_type='{WorkTrackingIntegrationType.pivotal.value}';
        """)
    op.execute(f"""
            insert into analytics.work_items_source_state_map (state, state_type, work_items_source_id)
            select 'unstarted', '{WorkItemsStateType.open.value}', 
            id from analytics.work_items_sources where integration_type='{WorkTrackingIntegrationType.pivotal.value}';
        """)
    op.execute(f"""
            insert into analytics.work_items_source_state_map (state, state_type, work_items_source_id)
            select 'planned', '{WorkItemsStateType.open.value}', 
            id from analytics.work_items_sources where integration_type='{WorkTrackingIntegrationType.pivotal.value}';
        """)
    op.execute(f"""
            insert into analytics.work_items_source_state_map (state, state_type, work_items_source_id)
            select 'started', '{WorkItemsStateType.wip.value}', 
            id from analytics.work_items_sources where integration_type='{WorkTrackingIntegrationType.pivotal.value}';
        """)
    op.execute(f"""
            insert into analytics.work_items_source_state_map (state, state_type, work_items_source_id)
            select 'finished', '{WorkItemsStateType.wip.value}', 
            id from analytics.work_items_sources where integration_type='{WorkTrackingIntegrationType.pivotal.value}';
        """)
    op.execute(f"""
            insert into analytics.work_items_source_state_map (state, state_type, work_items_source_id)
            select 'delivered', '{WorkItemsStateType.wip.value}', 
            id from analytics.work_items_sources where integration_type='{WorkTrackingIntegrationType.pivotal.value}';
        """)
    op.execute(f"""
            insert into analytics.work_items_source_state_map (state, state_type, work_items_source_id)
            select 'accepted', '{WorkItemsStateType.complete.value}', 
            id from analytics.work_items_sources where integration_type='{WorkTrackingIntegrationType.pivotal.value}';
    """)


def upgrade():
    op.create_table('work_items_source_state_map',
                    sa.Column('state', sa.String(), nullable=False),
                    sa.Column('state_type', sa.String(), server_default='open',
                              nullable=False),
                    sa.Column('work_items_source_id', sa.Integer(), nullable=False),
                    sa.ForeignKeyConstraint(['work_items_source_id'],
                                            ['analytics.work_items_sources.id'], ),
                    sa.PrimaryKeyConstraint('state', 'work_items_source_id'),
                    schema='analytics'
                    )
    # Run the data migration
    insert_default_state_maps()


def downgrade():
    op.drop_table('work_items_source_state_map', schema='analytics')
