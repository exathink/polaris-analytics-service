"""initial_teams_tables

Revision ID: 946776664520
Revises: 9ab1715c3b9f
Create Date: 2021-06-21 16:43:29.817513

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '946776664520'
down_revision = '9ab1715c3b9f'
branch_labels = None
depends_on = None


def upgrade():
    # Teams
    op.create_table('teams',
                    sa.Column('id', sa.BigInteger(), nullable=False),
                    sa.Column('key', postgresql.UUID(as_uuid=True), nullable=False),
                    sa.Column('name', sa.String(), nullable=False),
                    sa.Column('organization_id', sa.Integer(), nullable=False),
                    sa.ForeignKeyConstraint(['organization_id'], ['analytics.organizations.id'], ),
                    sa.PrimaryKeyConstraint('id'),
                    sa.UniqueConstraint('key'),
                    schema='analytics'
                    )
    op.create_index(op.f('ix_analytics_teams_organization_id'), 'teams', ['organization_id'], unique=False,
                    schema='analytics')

    # contributors teams
    op.create_table('contributors_teams',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('contributor_id', sa.Integer(), nullable=False),
    sa.Column('team_id', sa.Integer(), nullable=False),
    sa.Column('start_date', sa.DateTime(), nullable=False),
    sa.Column('end_date', sa.DateTime(), nullable=True),
    sa.Column('capacity', sa.Float(), server_default='1.0', nullable=True),
    sa.ForeignKeyConstraint(['contributor_id'], ['analytics.contributors.id'], ),
    sa.ForeignKeyConstraint(['team_id'], ['analytics.teams.id'], ),
    sa.PrimaryKeyConstraint('id'),
    schema='analytics'
    )
    op.create_index(op.f('ix_analytics_contributors_teams_contributor_id'), 'contributors_teams', ['contributor_id'], unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_contributors_teams_team_id'), 'contributors_teams', ['team_id'], unique=False, schema='analytics')

    # add current team assignment to contributors
    op.add_column('contributors', sa.Column('current_team_assignment_id', sa.Integer(), nullable=True), schema='analytics')
    op.create_index(op.f('ix_analytics_contributors_current_team_assignment_id'), 'contributors', ['current_team_assignment_id'], unique=False, schema='analytics')
    op.create_foreign_key(None, 'contributors', 'contributors_teams', ['current_team_assignment_id'], ['id'], source_schema='analytics', referent_schema='analytics')


def downgrade():
    op.drop_constraint(None, 'contributors', schema='analytics', type_='foreignkey')
    op.drop_index(op.f('ix_analytics_contributors_current_team_assignment_id'), table_name='contributors', schema='analytics')
    op.drop_column('contributors', 'current_team_assignment_id', schema='analytics')
    op.drop_index(op.f('ix_analytics_teams_organization_id'), table_name='teams', schema='analytics')
    op.drop_table('teams', schema='analytics')
    op.drop_index(op.f('ix_analytics_contributors_teams_team_id'), table_name='contributors_teams', schema='analytics')
    op.drop_index(op.f('ix_analytics_contributors_teams_contributor_id'), table_name='contributors_teams', schema='analytics')
    op.drop_table('contributors_teams', schema='analytics')
