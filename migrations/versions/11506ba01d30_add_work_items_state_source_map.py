"""add_work_items_state_source_map

Revision ID: 11506ba01d30
Revises: c74cb2b8db51
Create Date: 2020-01-22 09:49:35.484005

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '11506ba01d30'
down_revision = 'c74cb2b8db51'
branch_labels = None
depends_on = None


def upgrade():

    op.create_table('work_items_source_state_map',
    sa.Column('state', sa.String(), nullable=False),
    sa.Column('state_type', sa.String(), server_default='open', nullable=False),
    sa.Column('work_items_source_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['work_items_source_id'], ['analytics.work_items_sources.id'], ),
    sa.PrimaryKeyConstraint('state', 'work_items_source_id'),
    schema='analytics'
    )


def downgrade():
    op.drop_table('work_items_source_state_map', schema='analytics')
