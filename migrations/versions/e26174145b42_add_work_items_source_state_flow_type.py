"""add_work_items_source_state_flow_type

Revision ID: e26174145b42
Revises: bd6a648d9d9f
Create Date: 2022-11-08 20:57:28.733045

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e26174145b42'
down_revision = 'bd6a648d9d9f'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('work_items_source_state_map', sa.Column('flow_type', sa.String(), nullable=True), schema='analytics')


def downgrade():

    op.drop_column('work_items_source_state_map', 'flow_type', schema='analytics')

