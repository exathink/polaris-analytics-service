"""add_table_work_items_teams

Revision ID: 43cce74bc18d
Revises: 1f98b4288805
Create Date: 2021-06-28 14:46:59.594616

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '43cce74bc18d'
down_revision = '1f98b4288805'
branch_labels = None
depends_on = None


def upgrade():

    op.create_table('work_items_teams',
    sa.Column('work_item_id', sa.BigInteger(), nullable=False),
    sa.Column('team_id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['team_id'], ['analytics.teams.id'], ),
    sa.ForeignKeyConstraint(['work_item_id'], ['analytics.work_items.id'], ),
    sa.PrimaryKeyConstraint('work_item_id', 'team_id'),
    schema='analytics'
    )
    op.create_index(op.f('ix_analytics_work_items_teams_team_id'), 'work_items_teams', ['team_id'], unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_work_items_teams_work_item_id'), 'work_items_teams', ['work_item_id'], unique=False, schema='analytics')



def downgrade():

    op.drop_index(op.f('ix_analytics_work_items_teams_work_item_id'), table_name='work_items_teams', schema='analytics')
    op.drop_index(op.f('ix_analytics_work_items_teams_team_id'), table_name='work_items_teams', schema='analytics')
    op.drop_table('work_items_teams', schema='analytics')

