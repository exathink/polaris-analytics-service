"""ff_demo_mode

Revision ID: b2732636b7d4
Revises: 6781ced0d873
Create Date: 2022-08-23 20:30:00.245538

"""
from alembic import op
from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag


# revision identifiers, used by Alembic.
revision = 'b2732636b7d4'
down_revision = '6781ced0d873'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('system.demo_mode', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('system.demo_mode', join_this=session)
    session.commit()
