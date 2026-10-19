"""`python -m zfsbackup.cli` -- the same entry point as the `zfsbackup-config`
console script, for use from a checkout without installing it (parity with
`python -m zfsbackup.daemon`)."""

import sys

from zfsbackup.cli.main import main

sys.exit(main())
