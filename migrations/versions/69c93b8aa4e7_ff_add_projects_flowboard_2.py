"""ff_add_projects_flowboard-2

Revision ID: 69c93b8aa4e7
Revises: 13954158ff1c
Create Date: 2020-08-08 21:07:32.088946

"""
from alembic import op
from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag


# revision identifiers, used by Alembic.
revision = '69c93b8aa4e7'
down_revision = '13954158ff1c'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('projects.flowboard-2', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('projects.flowboard-2', join_this=session)
    session.commit()
