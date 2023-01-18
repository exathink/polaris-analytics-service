"""ff_age_latency_enhancements

Revision ID: 5f8d628fe575
Revises: 9b99d5370dc1
Create Date: 2023-01-16 18:30:07.572413

"""
from alembic import op


from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag

# revision identifiers, used by Alembic.
revision = '5f8d628fe575'
down_revision = '9b99d5370dc1'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('ui.age_latency_enhancements', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('ui.age_latency_enhancements', join_this=session)
    session.commit()
