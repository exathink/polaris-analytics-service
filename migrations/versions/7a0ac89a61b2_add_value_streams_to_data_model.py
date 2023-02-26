"""add value streams to data model

Revision ID: 7a0ac89a61b2
Revises: 5f8d628fe575
Create Date: 2023-02-25 15:14:05.592139

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '7a0ac89a61b2'
down_revision = '5f8d628fe575'
branch_labels = None
depends_on = None


def upgrade():

    op.create_table('value_streams',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('name', sa.String(length=256), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('project_id', sa.Integer(), nullable=False),
    sa.Column('work_item_selectors', postgresql.ARRAY(sa.String()), server_default='{}', nullable=False),
    sa.ForeignKeyConstraint(['project_id'], ['analytics.projects.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('key'),
    schema='analytics'
    )
    op.create_index(op.f('ix_analytics_value_streams_project_id'), 'value_streams', ['project_id'], unique=False, schema='analytics')


def downgrade():

    op.drop_index(op.f('ix_analytics_value_streams_project_id'), table_name='value_streams', schema='analytics')
    op.drop_table('value_streams', schema='analytics')

