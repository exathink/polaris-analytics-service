"""add_team_keys_to_commits

Revision ID: 1f98b4288805
Revises: ee485d84cd4a
Create Date: 2021-06-25 16:43:54.534194

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '1f98b4288805'
down_revision = 'ee485d84cd4a'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('commits', sa.Column('author_team_key', postgresql.UUID(as_uuid=True), nullable=True), schema='analytics')
    op.add_column('commits', sa.Column('committer_team_key', postgresql.UUID(as_uuid=True), nullable=True), schema='analytics')
    op.create_index(op.f('ix_analytics_commits_author_team_key'), 'commits', ['author_team_key'], unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_commits_committer_team_key'), 'commits', ['committer_team_key'], unique=False, schema='analytics')



def downgrade():

    op.drop_index(op.f('ix_analytics_commits_committer_team_key'), table_name='commits', schema='analytics')
    op.drop_index(op.f('ix_analytics_commits_author_team_key'), table_name='commits', schema='analytics')
    op.drop_column('commits', 'committer_team_key', schema='analytics')
    op.drop_column('commits', 'author_team_key', schema='analytics')
