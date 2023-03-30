"""add_custom_type_mappings_to_work_items_source

Revision ID: 7b620b806cf7
Revises: 7a0ac89a61b2
Create Date: 2023-03-30 13:36:47.437108

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '7b620b806cf7'
down_revision = '7a0ac89a61b2'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('work_items_sources', sa.Column('custom_type_mappings', postgresql.JSONB(astext_type=sa.Text()), server_default='[]', nullable=True), schema='analytics')
    # ### end Alembic commands ###


def downgrade():

    op.drop_column('work_items_sources', 'custom_type_mappings', schema='analytics')
    # ### end Alembic commands ###
