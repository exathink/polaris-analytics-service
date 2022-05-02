"""add_exclude_flag_to_project_repositories

Revision ID: ebc5bfd19e0b
Revises: abbd533c6dc7
Create Date: 2022-04-26 19:55:41.934740

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'ebc5bfd19e0b'
down_revision = 'abbd533c6dc7'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('projects_repositories', sa.Column('excluded', sa.Boolean(), server_default='FALSE', nullable=True),
                  schema='analytics')


def downgrade():
    op.drop_column('projects_repositories', 'excluded', schema='analytics')
