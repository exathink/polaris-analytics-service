"""add-settings-to-teams

Revision ID: c3c33e6fa9ce
Revises: 548148307549
Create Date: 2021-09-01 16:41:02.655374

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'c3c33e6fa9ce'
down_revision = '548148307549'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('teams',
                  sa.Column('settings', postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'"),
                            nullable=True), schema='analytics')


def downgrade():
    op.drop_column('teams', 'settings', schema='analytics')
