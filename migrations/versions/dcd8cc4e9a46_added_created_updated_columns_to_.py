"""Added created, updated columns to Accounts and Organizations

Revision ID: dcd8cc4e9a46
Revises: c149c9445482
Create Date: 2019-05-18 19:24:36.036455

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'dcd8cc4e9a46'
down_revision = 'c149c9445482'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('accounts', sa.Column('created', sa.DateTime(), nullable=True), schema='analytics')
    op.add_column('accounts', sa.Column('updated', sa.DateTime(), nullable=True), schema='analytics')
    op.add_column('organizations', sa.Column('created', sa.DateTime(), nullable=True), schema='analytics')
    op.add_column('organizations', sa.Column('updated', sa.DateTime(), nullable=True), schema='analytics')


def downgrade():

    op.drop_column('organizations', 'updated', schema='analytics')
    op.drop_column('organizations', 'created', schema='analytics')
    op.drop_column('accounts', 'updated', schema='analytics')
    op.drop_column('accounts', 'created', schema='analytics')
