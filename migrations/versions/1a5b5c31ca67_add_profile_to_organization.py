"""add_profile_to_organization

Revision ID: 1a5b5c31ca67
Revises: a636d61c1e41
Create Date: 2019-06-03 16:28:38.642911

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '1a5b5c31ca67'
down_revision = 'a636d61c1e41'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('organizations', sa.Column('profile', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=True), schema='analytics')


def downgrade():
    op.drop_column('organizations', 'profile', schema='analytics')

