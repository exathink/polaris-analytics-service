"""add_projects_work_items_source_relationship

Revision ID: dddf02c9b763
Revises: 1a5b5c31ca67
Create Date: 2019-06-28 17:24:40.790227

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'dddf02c9b763'
down_revision = '1a5b5c31ca67'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('work_items_sources', sa.Column('project_id', sa.Integer(), nullable=True), schema='analytics')
    op.create_foreign_key(None, 'work_items_sources', 'projects', ['project_id'], ['id'], source_schema='analytics', referent_schema='analytics')
    # ### end Alembic commands ###


def downgrade():

    op.drop_constraint(None, 'work_items_sources', schema='analytics', type_='foreignkey')
    op.drop_column('work_items_sources', 'project_id', schema='analytics')
    # ### end Alembic commands ###
