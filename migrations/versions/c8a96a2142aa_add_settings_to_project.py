"""add_settings_to_project

Revision ID: c8a96a2142aa
Revises: fae565191c31
Create Date: 2020-09-16 16:33:18.588820

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'c8a96a2142aa'
down_revision = 'fae565191c31'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('projects',
                  sa.Column('settings', postgresql.JSONB(astext_type=sa.Text()), server_default=sa.text("'{}'"),
                            nullable=True), schema='analytics')


def downgrade():
    op.drop_column('projects', 'settings', schema='analytics')
