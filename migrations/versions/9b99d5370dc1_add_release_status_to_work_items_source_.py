"""add_release_status_to_work_items_source_state_map

Revision ID: 9b99d5370dc1
Revises: 4b1818e0d0eb
Create Date: 2022-12-20 22:21:29.574230

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9b99d5370dc1'
down_revision = '4b1818e0d0eb'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items_source_state_map', sa.Column('release_status', sa.String(), nullable=True), schema='analytics')
    ###


def downgrade():
    op.drop_column('work_items_source_state_map', 'release_status', schema='analytics')

