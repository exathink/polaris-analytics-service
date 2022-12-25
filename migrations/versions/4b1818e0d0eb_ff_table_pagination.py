"""ff_table_pagination

Revision ID: 4b1818e0d0eb
Revises: e26174145b42
Create Date: 2022-12-05 20:29:17.510239

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag
# revision identifiers, used by Alembic.
revision = '4b1818e0d0eb'
down_revision = 'e26174145b42'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('ui.table_pagination', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('ui.table_pagination', join_this=session)
    session.commit()
