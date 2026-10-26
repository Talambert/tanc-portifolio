"""`python -m dataeng_core.cli` equivale ao comando `dataeng`."""

import sys

from dataeng_core.cli.main import main

sys.exit(main())
