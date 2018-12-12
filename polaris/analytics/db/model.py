# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar




from sqlalchemy import \
    Table, Column, Integer, String, Text, DateTime, \
    Boolean, ForeignKey

from sqlalchemy import select, and_, func, event
from sqlalchemy.orm import relationship, mapper
from sqlalchemy.types import TIMESTAMP
from sqlalchemy.dialects.postgresql import JSONB, UUID, ENUM

from polaris.common import db
from polaris.utils import datetime_utils
from polaris.utils.collections import find




Base = db.polaris_declarative_base(schema='analytics')

class CommitFact(Base):
    __tablename__ = 'commit_facts'

