"""add_releases_and_story_points_columns_to_work_items

Revision ID: 8a74ac90c2ca
Revises: 9f1c9164f5a8
Create Date: 2023-07-27 20:40:08.554789

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '8a74ac90c2ca'
down_revision = '9f1c9164f5a8'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('work_items', sa.Column('releases', postgresql.ARRAY(sa.String()), server_default='{}', nullable=True), schema='analytics')
    op.add_column('work_items', sa.Column('story_points', sa.Integer(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_items', 'story_points', schema='analytics')
    op.drop_column('work_items', 'releases', schema='analytics')
