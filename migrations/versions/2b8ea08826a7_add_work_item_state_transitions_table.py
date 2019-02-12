"""Add work_item_state_transitions table

Revision ID: 2b8ea08826a7
Revises: 2ceb7dbd6e1b
Create Date: 2019-02-12 00:19:55.464636

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '2b8ea08826a7'
down_revision = '2ceb7dbd6e1b'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('work_item_state_transitions',
    sa.Column('work_item_id', sa.BigInteger(), nullable=False),
    sa.Column('seq_no', sa.Integer(), server_default='0', nullable=False),
    sa.Column('created_at', sa.DateTime(), nullable=False),
    sa.Column('previous_state', sa.String(), nullable=True),
    sa.Column('state', sa.String(), nullable=False),
    sa.ForeignKeyConstraint(['work_item_id'], ['analytics.work_items.id'], ),
    sa.PrimaryKeyConstraint('work_item_id', 'seq_no'),
    schema='analytics'
    )
    op.add_column('work_items', sa.Column('next_state_seq_no', sa.Integer(), server_default='0', nullable=False), schema='analytics')



def downgrade():

    op.drop_column('work_items', 'next_state_seq_no', schema='analytics')
    op.drop_table('work_item_state_transitions', schema='analytics')

