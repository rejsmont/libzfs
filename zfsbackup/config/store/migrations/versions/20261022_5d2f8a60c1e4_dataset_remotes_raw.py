"""dataset_remotes.raw: per-destination send mode

Milestone B (decision B3). Adds `dataset_remotes.raw`, a nullable BOOLEAN that
mirrors `RemoteDatasetConfig.raw`: NULL = auto ("raw if and only if the
dataset is encrypted"), 1 = raw required, 0 = never raw.

The column is nullable with NO server default on purpose: every existing row
must come out NULL (auto), and a default of 0 or 1 would turn "auto" into a
hard stream-format choice for a server copy that cannot change format without
a full re-send. No CHECK constraint, matching `datasets.enabled`: the ORM
refuses anything but None/True/False on write and the CLI is the only writer;
a value written by hand outside the ORM is read with SQLite truthiness.

Downgrade drops the column (a table rebuild through `batch_alter_table`;
`dataset_remotes` is the parent of `retention_rules`' composite FK). Run it
with foreign keys off (SQLite's default for a bare `alembic` engine) and check
`PRAGMA foreign_key_check` afterwards: a bare `alembic downgrade` runs no FK
check of its own. The per-destination `raw` choices are lost; acceptable while
nothing is deployed.

Revision ID: 5d2f8a60c1e4
Revises: 7c3e9d51b2a8
Create Date: 2026-10-22 10:12:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '5d2f8a60c1e4'
down_revision: Union[str, Sequence[str], None] = '7c3e9d51b2a8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table('dataset_remotes', schema=None) as batch_op:
        batch_op.add_column(sa.Column('raw', sa.Boolean(), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table('dataset_remotes', schema=None) as batch_op:
        batch_op.drop_column('raw')
