"""A fake `zfs` executable for the tests that exercise the ZFS side of
`zfsbackup-config rename`. It never touches a real pool: its whole world is a
JSON file.

    $FAKEZFS_STATE   JSON ``{dataset name: {property: value}}``; read and
                     rewritten on every call
    $FAKEZFS_LOG     one JSON line (the argv after ``zfs``) appended per call
    $FAKEZFS_SLOW_INHERIT  seconds an `inherit` sleeps before acting, after appending a line
                     to $FAKEZFS_MARKER (lets a test act mid-inherit)
    $FAKEZFS_SLOW_SET      the same for `set`
    $FAKEZFS_FAIL    optional JSON list of ``[subcommand, dataset]`` or
                     ``[subcommand, dataset, property]`` rules; a matching call fails with a message on stderr and exit 1

Supported: ``list`` (a dataset exists or "dataset does not exist"; also the name-only
cross-check ``zfs.get.rows`` runs), ``get`` (the two phases of the property readers: ``-o name,property,source -s ...`` lists the
dataset's properties, all local, with no ``type`` row; ``-o name,property,value,source`` prints either the ``type`` row
and every property for ``all`` or exactly ``name``, ``type`` and the requested ones, unset ones as ``-``; ``-o value``
reads one), ``set PROP=VALUE DATASET`` and
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
        columns = args[args.index("-o") + 1] if "-o" in args else "name,property,value,received,source"
        if columns == "value":                      # zfs.get.value: `get -H -o value [-s S] <prop> <ds>`
            print(state[name].get(args[-2], "-"))
            sys.exit(0)
        if columns == "name,property,source":       # phase 1: `-s local,received` drops the `type` row
            sources = args[args.index("-s") + 1].split(",") if "-s" in args else None
            rows = [] if sources else [("type", "-")]
            rows += [(prop, "local") for prop in state[name]] if (sources is None or "local" in sources) else []
            for prop, source in rows:
                print("\t".join((name, prop, source)))
            sys.exit(0)
        with_received = columns == "name,property,value,received,source"
        assert with_received or columns == "name,property,value,source", columns
        plist = args[-2]
        if plist == "all":
            # `get ... all <ds>` opens with the `type` row (there is no `name` row for `all`), then the properties.
            rows = [("type", "filesystem", "-")] + [(p, v, "local") for p, v in state[name].items()]
        else:
            # An explicit list prints EXACTLY the requested rows, `name` and `type` first; unset ones are `-`.
            rows = [("name", name, "-"), ("type", "filesystem", "-")] + [
                (p, state[name][p], "local") if p in state[name] else (p, "-", "-") for p in plist.split(",")[2:]
            ]
        for prop, value, source in rows:
            extra = ("-",) if with_received else ()
            print("\t".join((name, prop, value) + extra + (source,)))
    sys.exit(0)

if command == "set":
    delay = os.environ.get("FAKEZFS_SLOW_SET")
    if delay:
        marker = os.environ.get("FAKEZFS_MARKER")
        if marker:
            with open(marker, "a", encoding="utf-8") as handle:
                handle.write("set started\n")
        import time

        time.sleep(float(delay))
        with open(state_path, encoding="utf-8") as handle:
            state = json.load(handle)
    prop, value = args[1].split("=", 1)
    state[name][prop] = value
    save()
    sys.exit(0)

if command == "inherit":
    delay = os.environ.get("FAKEZFS_SLOW_INHERIT")
    if delay:
        # A window for a test to act while the CLI is "in" a zfs inherit: say
        # so (the marker file), then take a while.
        marker = os.environ.get("FAKEZFS_MARKER")
        if marker:
            with open(marker, "a", encoding="utf-8") as handle:
                handle.write("inherit started\n")
        import time

        time.sleep(float(delay))
        with open(state_path, encoding="utf-8") as handle:
            state = json.load(handle)  # re-read: nothing else writes, but be safe
    state[name].pop(args[1], None)
    save()
    sys.exit(0)

sys.stderr.write(f"fake zfs: unsupported command {args}\n")
sys.exit(2)
