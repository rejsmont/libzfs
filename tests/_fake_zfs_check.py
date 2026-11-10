"""A read-only fake `zfs` for the `zfsbackup-config check` tests: it answers
`zfs list -t snapshot` and `zfs get -H -o value <prop> <dataset>` from a JSON
spec and logs every call. Anything else (a write, a send, a destroy) exits 2
and is logged, so a test can assert that `check` made no other call.

    $FAKECHECK_SPEC  JSON {"snapshots": {dataset: [[short name, createtxg], ...]},
                           "props": {dataset: {property: value}},
                           "datasets": {name: "filesystem"|"volume"},   # optional: the received-datasets tree
                           "fail": [dataset, ...],      # `list` of these exits 1
                           "markers": {dataset: {property: "local"|"received"}},   # backup-copy markers (A3)
                           "fail_names": [root, ...],   # the name-only `list -r` of these exits 1 (A3 discovery)
                           "fail_markers": [root, ...], # the copy-marker read below these exits 1 (A3)
                           "fail_get": [dataset | "dataset::property", ...]}  # `get -H -o value` of these exits 1
                           # (an unreadable anchor / pin: the whole dataset, or just one property)

A server's `check` also lists the target (`list -H [-r] -t filesystem[,volume] -o name,type <target>`, from
"datasets") and reads the received datasets' `client_id`/`config` in ONE batch (B-1d):
`get -H -o name,property,value,source -r -t filesystem,volume name,type,<p1>,<p2> <target>`, printed the way
zfs does: `name`, `type`, then each requested property in order, `-` when unset, every set one `local`.
A recursive dataset's descendants (A3) are named by `list -H -r -t filesystem,volume -o name,type <parent>` (from
"datasets") and its backup-copy markers by the value-free
`get -H -o name,property,source -s local,received,inherited -r -t filesystem,volume <props> <parent>`: one
`name<TAB>property<TAB>source` row per marker a dataset carries (`local`, `received`) or inherits from the nearest
ancestor that has it (`inherited from <ancestor>`), from "markers", the dataset and everything below it, a
dataset's rows together; no value is ever printed.
    $FAKECHECK_LOG   one JSON line (argv after `zfs`) appended per call
"""

import json
import os
import sys

args = sys.argv[1:]
with open(os.environ["FAKECHECK_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps(args) + "\n")
with open(os.environ["FAKECHECK_SPEC"], encoding="utf-8") as handle:
    spec = json.load(handle)

def flag(name):
    return args[args.index(name) + 1] if name in args else None


def under(target, recursive):
    return [n for n in spec.get("datasets", {}) if n == target or (recursive and n.startswith(target + "/"))]


if args[:1] == ["list"] and flag("-o") == "name,type" and flag("-t") in ("filesystem", "filesystem,volume"):
    target = args[-1]
    if target in spec.get("fail", []):
        sys.stderr.write(f"cannot open '{target}': dataset does not exist\n")
        sys.exit(1)
    if "-r" in args and target in spec.get("fail_names", []):
        sys.stderr.write(f"cannot open '{target}': pool I/O is currently suspended\n")
        sys.exit(1)
    wanted = flag("-t").split(",")
    for name in under(target, "-r" in args):
        if spec["datasets"][name] in wanted:
            print(f"{name}\t{spec['datasets'][name]}")
    sys.exit(0)

if args[:3] == ["get", "-H", "-o"] and flag("-o") == "name,property,value,source":
    target, requested = args[-1], args[-2].split(",")
    assert "-r" in args and requested[:2] == ["name", "type"], args
    for name in under(target, True):
        print(f"{name}\tname\t{name}\t-")
        print(f"{name}\ttype\t{spec['datasets'][name]}\t-")
        for prop in requested[2:]:
            value = spec.get("props", {}).get(name, {}).get(prop)
            print(f"{name}\t{prop}\t-\t-" if value is None else f"{name}\t{prop}\t{value}\tlocal")
    sys.exit(0)

def marker_source(name, prop):
    """`local` / `received` / `inherited from <ancestor>` for a backup-copy marker, else None."""
    markers = spec.get("markers", {})
    own = markers.get(name, {}).get(prop)
    if own is not None:
        return own
    parent = name.rpartition("/")[0]
    while parent:
        if prop in markers.get(parent, {}):
            return f"inherited from {parent}"
        parent = parent.rpartition("/")[0]
    return None


if args[:3] == ["get", "-H", "-o"] and flag("-o") == "name,property,source":
    assert "-r" in args and "-s" in args, args
    root, requested = args[-1], args[-2].split(",")
    wanted = flag("-s").split(",")
    if root in spec.get("fail_markers", []):
        sys.stderr.write(f"cannot open '{root}': pool I/O is currently suspended\n")
        sys.exit(1)
    for name in under(root, True):
        for prop in requested:
            source = marker_source(name, prop)
            if source is not None and source.split(" ")[0] in wanted:
                print(f"{name}\t{prop}\t{source}")
    sys.exit(0)

if args[:3] == ["list", "-H", "-t"] and args[3] == "snapshot":
    dataset = args[-1]
    if dataset in spec.get("fail", []):
        sys.stderr.write(f"cannot open '{dataset}': dataset does not exist\n")
        sys.exit(1)
    for index, (name, txg) in enumerate(spec.get("snapshots", {}).get(dataset, [])):
        print("\t".join([f"{dataset}@{name}", "snapshot", str(txg), str(1000 + txg), "0", "-", "off", "none"]))
    sys.exit(0)

if args[:4] == ["get", "-H", "-o", "value"]:
    prop, dataset = args[-2], args[-1]
    if dataset in spec.get("fail_get", []) or f"{dataset}::{prop}" in spec.get("fail_get", []):
        sys.stderr.write(f"cannot open '{dataset}': pool I/O is currently suspended\n")
        sys.exit(1)
    print(spec.get("props", {}).get(dataset, {}).get(prop, "-"))
    sys.exit(0)

sys.stderr.write(f"fake zfs (check): unsupported command {args}\n")
sys.exit(2)
