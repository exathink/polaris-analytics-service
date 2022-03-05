"""add_index_commits_by_author_date

Revision ID: 772783853551
Revises: f34d4cdc8f90
Create Date: 2022-03-05 15:54:41.018330

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '772783853551'
down_revision = 'f34d4cdc8f90'
branch_labels = None
depends_on = None


def upgrade():
    op.create_index(op.f('ix_analytics_commits_author_date'), 'commits', ['author_date'], unique=False, schema='analytics')



def downgrade():
    op.drop_index(op.f('ix_analytics_commits_author_date'), table_name='commits', schema='analytics')

