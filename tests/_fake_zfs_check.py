"""A read-only fake `zfs` for the `zfsbackup-config check` tests: it answers
`zfs list -t snapshot` and `zfs get -H -o value <prop> <dataset>` from a JSON
spec and logs every call. Anything else (a write, a send, a destroy) exits 2
and is logged, so a test can assert that `check` made no other call.

    $FAKECHECK_SPEC  JSON {"snapshots": {dataset: [[short name, createtxg], ...]},
                           "props": {dataset: {property: value}},
                           "fail": [dataset, ...]}      # `list` of these exits 1
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
    print(spec.get("props", {}).get(dataset, {}).get(prop, "-"))
    sys.exit(0)

sys.stderr.write(f"fake zfs (check): unsupported command {args}\n")
sys.exit(2)
