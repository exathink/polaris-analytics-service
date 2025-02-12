"""add_table_teams_repositories

Revision ID: 194fb3b1ab35
Revises: 43cce74bc18d
Create Date: 2021-07-06 00:57:52.595422

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '194fb3b1ab35'
down_revision = '43cce74bc18d'
branch_labels = None
depends_on = None



def upgrade():
    op.create_table('teams_repositories',
                    sa.Column('repository_id', sa.Integer(), nullable=False),
                    sa.Column('team_id', sa.Integer(), nullable=False),
                    sa.Column('earliest_commit', sa.DateTime(), nullable=True),
                    sa.Column('latest_commit', sa.DateTime(), nullable=True),
                    sa.Column('commit_count', sa.Integer(), nullable=True),
                    sa.ForeignKeyConstraint(['repository_id'], ['analytics.repositories.id'], ondelete='CASCADE'),
                    sa.ForeignKeyConstraint(['team_id'], ['analytics.teams.id'], ondelete='CASCADE'),
                    sa.PrimaryKeyConstraint('repository_id', 'team_id'),
                    schema='analytics'
                    )

    op.create_index(op.f('ix_analytics_teams_repositories_repository_id'), 'teams_repositories', ['repository_id'],
                    unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_teams_repositories_team_id'), 'teams_repositories', ['team_id'], unique=False,
                    schema='analytics')





def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_analytics_teams_repositories_team_id'), table_name='teams_repositories', schema='analytics')
    op.drop_index(op.f('ix_analytics_teams_repositories_repository_id'), table_name='teams_repositories',
                  schema='analytics')
    op.drop_table('teams_repositories', schema='analytics')
    # ### end Alembic commands ###
