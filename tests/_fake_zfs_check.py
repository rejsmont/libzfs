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

The descendants of a recursive dataset are read in BATCHES (one set of reads per parent, not per child):

  * `list -H -r -t snapshot -o name,type,createtxg,... <parent>`: the snapshots of the parent and everything below
    it, from "snapshots", one row per snapshot (a dataset's snapshots together, datasets in "datasets" order);
  * phase 1 `get -H -o name,property,source -s local,received -r -t filesystem,volume all <parent>`: one row per
    property a dataset carries from "props", whose source is "prop_sources"[dataset][property] (default `local`);
  * phase 2 `get -H -o name,property,value,source -r -t filesystem,volume name,type,<p>... <parent>`: `name`, `type`,
    then each requested property, `-`/`-` when unset. A property listed in "inherit" is printed by a dataset that
    has none of its own as `inherited from <nearest ancestor holding it>`.

and the faults a test can inject, per PARENT (the batch is the unit):

    "fail_tree": [parent, ...]         # the recursive snapshot listing exits 1
    "garbage_tree": [parent, ...]      # ... prints a row nobody can trust (strict listing refuses it)
    "fail_props1": [parent, ...]       # phase 1 exits 1
    "fail_rows": [parent, ...]         # phase 2 exits 1 (a received server's batch read too)
    "vanish": {dataset: [prop, ...]}   # phase 2 prints them unset though phase 1 listed them: a disagreement, always
    "vanish_once": {dataset: [prop, ...]}   # ... only the first time (the second read agrees)
    "gone_after_first": {parent: [name, ...]}   # the name-only listing of parent omits these from its 2nd call on
    "fail_names_again": [parent, ...]  # ... and exits 1 from its 2nd call on (the re-listing after the expansion)
    "slow": {"<subcommand>:<last argument>": seconds}   # sleep first ("*" = any subcommand): a hung pool

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
import time

args = sys.argv[1:]
with open(os.environ["FAKECHECK_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps(args) + "\n")
with open(os.environ["FAKECHECK_SPEC"], encoding="utf-8") as handle:
    spec = json.load(handle)

def flag(name):
    return args[args.index(name) + 1] if name in args else None


def call_number():
    """How many calls with exactly this argv the log holds, this one included."""
    with open(os.environ["FAKECHECK_LOG"], encoding="utf-8") as handle:
        return sum(1 for line in handle if json.loads(line) == args)


for key, seconds in spec.get("slow", {}).items():
    sub, _, last = key.partition(":")
    if sub in ("*", args[0]) and last == args[-1]:
        time.sleep(seconds)


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
    again = call_number() >= 2
    if "-r" in args and again and target in spec.get("fail_names_again", []):
        sys.stderr.write(f"cannot open '{target}': pool I/O is currently suspended\n")
        sys.exit(1)
    gone = spec.get("gone_after_first", {}).get(target, []) if again else []
    wanted = flag("-t").split(",")
    for name in under(target, "-r" in args):
        if spec["datasets"][name] in wanted and name not in gone:
            print(f"{name}\t{spec['datasets'][name]}")
    sys.exit(0)

def prop_source(name, prop):
    return spec.get("prop_sources", {}).get(name, {}).get(prop, "local")


def inherited_from(name, prop):
    if prop not in spec.get("inherit", []):
        return None
    parent = name.rpartition("/")[0]
    while parent:
        if prop in spec.get("props", {}).get(parent, {}):
            return parent
        parent = parent.rpartition("/")[0]
    return None


def vanished(name):
    """The properties phase 2 prints as unset though phase 1 listed them; `vanish_once` only the first time."""
    gone = list(spec.get("vanish", {}).get(name, []))
    once = spec.get("vanish_once", {}).get(name)
    if once:
        gone += once
        del spec["vanish_once"][name]
        with open(os.environ["FAKECHECK_SPEC"], "w", encoding="utf-8") as handle:
            json.dump(spec, handle)
    return gone


if args[:3] == ["get", "-H", "-o"] and flag("-o") == "name,property,value,source":
    target, requested = args[-1], args[-2].split(",")
    assert "-r" in args and requested[:2] == ["name", "type"], args
    if target in spec.get("fail_rows", []):
        sys.stderr.write(f"cannot open '{target}': pool I/O is currently suspended\n")
        sys.exit(1)
    for name in under(target, True):
        gone = vanished(name)
        print(f"{name}\tname\t{name}\t-")
        print(f"{name}\ttype\t{spec['datasets'][name]}\t-")
        for prop in requested[2:]:
            value = None if prop in gone else spec.get("props", {}).get(name, {}).get(prop)
            origin = inherited_from(name, prop) if value is None and prop not in gone else None
            if value is not None:
                print(f"{name}\t{prop}\t{value}\t{prop_source(name, prop)}")
            elif origin is not None:
                print(f"{name}\t{prop}\t{spec['props'][origin][prop]}\tinherited from {origin}")
            else:
                print(f"{name}\t{prop}\t-\t-")
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


if args[:3] == ["get", "-H", "-o"] and flag("-o") == "name,property,source" and args[-2] == "all":
    assert "-r" in args and "-s" in args, args
    root = args[-1]
    wanted = flag("-s").split(",")
    if root in spec.get("fail_props1", []):
        sys.stderr.write(f"cannot open '{root}': pool I/O is currently suspended\n")
        sys.exit(1)
    for name in under(root, True):
        for prop in spec.get("props", {}).get(name, {}):
            if prop_source(name, prop).split(" ")[0] in wanted:
                print(f"{name}\t{prop}\t{prop_source(name, prop)}")
    sys.exit(0)

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

if args[:2] == ["list", "-H"] and flag("-t") == "snapshot":
    root = args[-1]
    if root in spec.get("fail", []):
        sys.stderr.write(f"cannot open '{root}': dataset does not exist\n")
        sys.exit(1)
    snapshots = spec.get("snapshots", {})
    if "-r" in args:
        # the batch listing of a recursive parent's whole tree (one exec per parent)
        if root in spec.get("fail_tree", []):
            sys.stderr.write(f"cannot open '{root}': pool I/O is currently suspended\n")
            sys.exit(1)
        if root in spec.get("garbage_tree", []):
            print("garbage with the wrong number of columns")
            sys.exit(0)
        order = list(spec.get("datasets", {}))
        below = [n for n in snapshots if n == root or n.startswith(root + "/")]
        below.sort(key=lambda n: (order.index(n) if n in order else len(order), n))
        for number, dataset in enumerate(below):
            for name, txg in snapshots[dataset]:
                print("\t".join([f"{dataset}@{name}", "snapshot", str(txg), str(1000 + txg + 10000 * number),
                                 "0", "-", "off", "none"]))
        sys.exit(0)
    for index, (name, txg) in enumerate(snapshots.get(root, [])):
        print("\t".join([f"{root}@{name}", "snapshot", str(txg), str(1000 + txg), "0", "-", "off", "none"]))
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
