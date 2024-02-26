"""add_ff_network_viz

Revision ID: 7c247d4dc855
Revises: 0ad7ba8a77e1
Create Date: 2024-02-26 20:44:04.491674

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag

# revision identifiers, used by Alembic.
revision = '7c247d4dc855'
down_revision = '0ad7ba8a77e1'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('ui.network_viz', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('ui.network_viz', join_this=session)
    session.commit()

