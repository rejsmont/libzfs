"""A fake `zfs` executable for the tests that exercise the ZFS side of
`zfsbackup-config rename`. It never touches a real pool: its whole world is a
JSON file.

    $FAKEZFS_STATE   JSON ``{dataset name: {property: value}}``; read and
                     rewritten on every call
    $FAKEZFS_LOG     one JSON line (the argv after ``zfs``) appended per call
    $FAKEZFS_FAIL    optional JSON list of ``[subcommand, dataset]`` or
                     ``[subcommand, dataset, property]`` rules; a matching call fails with a message on stderr and exit 1

Supported: ``list`` (a dataset exists or "dataset does not exist"), ``get``
(every property of the dataset as a local one), ``set PROP=VALUE DATASET`` and
``inherit PROP [-r] [-S] DATASET``. Anything else exits 2 so a test notices
that the code under test asked for something it should not.
"""

import json
import os
import sys

state_path = os.environ["FAKEZFS_STATE"]
log_path = os.environ["FAKEZFS_LOG"]
args = sys.argv[1:]

with open(log_path, "a", encoding="utf-8") as log:
    log.write(json.dumps(args) + "\n")

with open(state_path, encoding="utf-8") as handle:
    state = json.load(handle)


def save() -> None:
    with open(state_path, "w", encoding="utf-8") as handle:
        json.dump(state, handle)


if not args:
    sys.exit(2)
command, name = args[0], args[-1]

def _property_of(argv):
    """The property a `set` / `inherit` call is about (None otherwise)."""
    if argv[0] == "set":
        return argv[1].split("=", 1)[0]
    if argv[0] == "inherit":
        return argv[1]
    return None


for rule in json.loads(os.environ.get("FAKEZFS_FAIL", "[]")):
    failing_command, failing_name = rule[0], rule[1]
    failing_prop = rule[2] if len(rule) > 2 else None
    if (
        failing_command == command
        and failing_name == name
        and (failing_prop is None or failing_prop == _property_of(args))
    ):
        sys.stderr.write(f"fake zfs: {command} {name} failed on purpose\n")
        sys.exit(1)

if command in ("list", "get"):
    if name not in state:
        sys.stderr.write(f"cannot open '{name}': dataset does not exist\n")
        sys.exit(1)
    if command == "list":
        columns = args[args.index("-o") + 1].split(",") if "-o" in args else ["name"]
        values = {"name": name, "type": "filesystem"}
        print("\t".join(values.get(c, "-") for c in columns))
    else:
        for prop, value in state[name].items():
            print(f"{name}\t{prop}\t{value}\t-\tlocal")
    sys.exit(0)

if command == "set":
    prop, value = args[1].split("=", 1)
    state[name][prop] = value
    save()
    sys.exit(0)

if command == "inherit":
    state[name].pop(args[1], None)
    save()
    sys.exit(0)

sys.stderr.write(f"fake zfs: unsupported command {args}\n")
sys.exit(2)
