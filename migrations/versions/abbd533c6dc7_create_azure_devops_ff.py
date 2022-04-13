"""create_azure_devops_ff

Revision ID: abbd533c6dc7
Revises: 772783853551
Create Date: 2022-04-13 14:22:54.779893

"""
from alembic import op
from sqlalchemy import orm
from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag

# revision identifiers, used by Alembic.
revision = 'abbd533c6dc7'
down_revision = '772783853551'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('connectors.azure-devops', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('connectors.azure-devops', join_this=session)
    session.commit()
