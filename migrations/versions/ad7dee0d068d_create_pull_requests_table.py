"""create_pull_requests_table

Revision ID: ad7dee0d068d
Revises: 254d4cad11db
Create Date: 2020-06-23 12:42:30.624766

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'ad7dee0d068d'
down_revision = '9d94f91512d8'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('pull_requests',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('source_id', sa.String(), nullable=False),
    sa.Column('display_id', sa.String(), nullable=True),
    sa.Column('title', sa.String(length=256), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('web_url', sa.String(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('deleted_at', sa.DateTime(), nullable=True),
    sa.Column('state', sa.String(), nullable=False),
    sa.Column('merge_status', sa.String(), nullable=True),
    sa.Column('merged_at', sa.DateTime(), nullable=True),
    sa.Column('source_branch', sa.String(), nullable=False),
    sa.Column('target_branch', sa.String(), nullable=False),
    sa.Column('source_repository_id', sa.Integer(), nullable=True),
    sa.Column('source_branch_latest_commit', sa.String(), nullable=True),
    sa.Column('repository_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['repository_id'], ['analytics.repositories.id'], ),
    sa.ForeignKeyConstraint(['source_repository_id'], ['analytics.repositories.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('key'),
    sa.UniqueConstraint('repository_id', 'source_id'),
    schema='analytics'
    )


def downgrade():
    op.drop_table('pull_requests', schema='analytics')
