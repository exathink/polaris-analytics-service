"""add_team_ids_to_commits

Revision ID: ee485d84cd4a
Revises: 946776664520
Create Date: 2021-06-23 21:10:18.168039

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ee485d84cd4a'
down_revision = '946776664520'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('commits', sa.Column('author_team_id', sa.Integer(), nullable=True), schema='analytics')
    op.add_column('commits', sa.Column('committer_team_id', sa.Integer(), nullable=True), schema='analytics')
    op.create_index(op.f('ix_analytics_commits_author_team_id'), 'commits', ['author_team_id'], unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_commits_committer_team_id'), 'commits', ['committer_team_id'], unique=False, schema='analytics')
    op.create_foreign_key('commits_author_team_id_fk', 'commits', 'teams', ['author_team_id'], ['id'], source_schema='analytics', referent_schema='analytics')
    op.create_foreign_key('commits_committer_team_id_fk', 'commits', 'teams', ['committer_team_id'], ['id'], source_schema='analytics', referent_schema='analytics')



def downgrade():

    op.drop_constraint('commits_committer_team_id_fk', 'commits', schema='analytics', type_='foreignkey')
    op.drop_constraint('commits_author_team_id_fk', 'commits', schema='analytics', type_='foreignkey')
    op.drop_index(op.f('ix_analytics_commits_committer_team_id'), table_name='commits', schema='analytics')
    op.drop_index(op.f('ix_analytics_commits_author_team_id'), table_name='commits', schema='analytics')
    op.drop_column('commits', 'committer_team_id', schema='analytics')
    op.drop_column('commits', 'author_team_id', schema='analytics')

