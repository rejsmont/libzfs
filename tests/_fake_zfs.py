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
                     ``[subcommand, dataset, property]`` rules (for ``list``/``get`` the third item is ``-r`` to fail only
                     the recursive call); a matching call fails with a message on stderr and exit 1

Supported: ``list`` (a dataset exists or "dataset does not exist"; also the name-only
cross-check ``zfs.get.rows`` runs), ``get`` (the two phases of the property readers: ``-o name,property,source -s ...`` lists the
dataset's properties, all local, with no ``type`` row; ``-o name,property,value,source`` prints either the ``type`` row
and every property for ``all`` or exactly ``name``, ``type`` and the requested ones, unset ones as ``-``; ``-o value``
reads one), ``set PROP=VALUE DATASET`` and
``inherit [-r] [-S] PROP DATASET`` (the flags may also follow PROP, as libzfseasy writes them). Anything else exits 2 so a test notices
that the code under test asked for something it should not.

A property's value in the state is a plain string (a LOCAL value, the default)
or ``{"value": V, "source": S}`` with ``S`` one of ``local``, ``received`` or
``inherited from <dataset>`` (`FakeZfs.set_prop`). The source is honoured by
``-s`` (``get -H -o value -s local`` prints ``-`` for a received or inherited value, and the
``name,property,source`` listing keeps only the requested sources) and printed in the source
column. ``set`` always makes the value local, and keeps a RECEIVED value it replaces as the property's
``received`` entry (``{"value": V, "source": "local", "received": R}``, also settable by `FakeZfs.set_prop(...,
received=R)`), as ZFS does. ``inherit`` removes only a LOCAL value and never touches a received-only one; a LOCAL value
that shadows a received one is, with plain ``inherit``, simply gone (the received value is hidden: the property then
reads as unset), but with ``-S`` it REVERTS to the received value (``source`` received), and with no received value
``-S`` behaves like a plain inherit. ``-r`` applies the inherit to the dataset and every dataset below it.

``-r`` (``list -H -r -t filesystem,volume -o name,type PARENT`` and ``get -H -o
name,property,source -s ... -r -t filesystem,volume all PARENT``) walks PARENT and every
dataset in the state below it, sorted by path; every dataset is a ``filesystem``. The recursive
``get`` therefore returns the sourced properties of every dataset in the walk, and a
backup copy root is just a dataset whose state carries a `org.zfsbackup:client_id` /
`org.zfsbackup:source_dataset` with the wanted source; with none in the state it finds none.
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
    """The property a `set` / `inherit` call is about; for `list` / `get` the word
    ``-r`` when the call is recursive (so a rule ``["list", "ds", "-r"]`` fails the
    descendant listing but not the single-dataset lookup), else ``-``."""
    if argv[0] in ("list", "get"):
        return "-r" if "-r" in argv else "-"
    if argv[0] == "set":
        return argv[1].split("=", 1)[0]
    if argv[0] == "inherit":
        return next(a for a in argv[1:-1] if not a.startswith("-"))
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

def _value(raw):
    return raw["value"] if isinstance(raw, dict) else raw


def _source(raw):
    return raw.get("source", "local") if isinstance(raw, dict) else "local"


def _wanted(argv):
    """The sources `-s` asked for (as a set of kinds), or None."""
    if "-s" not in argv:
        return None
    return {s.split()[0] for s in argv[argv.index("-s") + 1].split(",")}


def _walk(parent):
    """PARENT and every dataset in the state below it, parents first."""
    return sorted(
        (n for n in state if n == parent or n.startswith(parent + "/")),
        key=lambda n: n.split("/"),
    )


if command in ("list", "get"):
    if name not in state:
        sys.stderr.write(f"cannot open '{name}': dataset does not exist\n")
        sys.exit(1)
    if command == "list":
        columns = args[args.index("-o") + 1].split(",") if "-o" in args else ["name"]
        for member in (_walk(name) if "-r" in args else [name]):
            values = {"name": member, "type": "filesystem"}
            print("\t".join(values.get(c, "-") for c in columns))
    else:
        columns = args[args.index("-o") + 1] if "-o" in args else "name,property,value,received,source"
        if columns == "value":                      # zfs.get.value: `get -H -o value [-s S] <prop> <ds>`
            raw = state[name].get(args[-2])
            wanted = _wanted(args)
            if raw is None or (wanted is not None and _source(raw).split()[0] not in wanted):
                print("-")
            else:
                print(_value(raw))
            sys.exit(0)
        if columns == "name,property,source":       # phase 1: `-s local,received` drops the `type` row
            sources = _wanted(args)
            asked = args[-2]                        # `all`, or the explicit list (printed exactly, never more)
            for member in (_walk(name) if "-r" in args else [name]):
                rows = [] if sources else [("type", "-")]
                rows += [
                    (prop, _source(raw)) for prop, raw in state[member].items()
                    if (sources is None or _source(raw).split()[0] in sources)
                    and (asked == "all" or prop in asked.split(","))
                ]
                for prop, source in rows:
                    print("\t".join((member, prop, source)))
            sys.exit(0)
        with_received = columns == "name,property,value,received,source"
        assert with_received or columns == "name,property,value,source", columns
        plist = args[-2]
        if plist == "all":
            # `get ... all <ds>` opens with the `type` row (there is no `name` row for `all`), then the properties.
            rows = [("type", "filesystem", "-")] + [(p, _value(v), _source(v)) for p, v in state[name].items()]
        else:
            # An explicit list prints EXACTLY the requested rows, `name` and `type` first; unset ones are `-`.
            rows = [("name", name, "-"), ("type", "filesystem", "-")] + [
                (p, _value(state[name][p]), _source(state[name][p])) if p in state[name] else (p, "-", "-")
                for p in plist.split(",")[2:]
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
    previous = state[name].get(prop)
    received = (
        previous["received"] if isinstance(previous, dict) and "received" in previous
        else _value(previous) if previous is not None and _source(previous) == "received"
        else None
    )
    state[name][prop] = value if received is None else {"value": value, "source": "local", "received": received}
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
    flags = [a for a in args[1:-1] if a.startswith("-")]
    prop = next(a for a in args[1:-1] if not a.startswith("-"))
    for member in (_walk(name) if "-r" in flags else [name]):
        raw = state[member].get(prop)
        if raw is None or _source(raw) != "local":
            continue
        if "-S" in flags and isinstance(raw, dict) and "received" in raw:
            state[member][prop] = {"value": raw["received"], "source": "received"}
        else:
            state[member].pop(prop)
    save()
    sys.exit(0)

sys.stderr.write(f"fake zfs: unsupported command {args}\n")
sys.exit(2)
