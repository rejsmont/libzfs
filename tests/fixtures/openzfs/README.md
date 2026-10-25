# OpenZFS output captures

Real `zfs` / `zpool` output from OpenZFS **2.1.5**, **2.2.2** and **2.4.1** (Multipass VMs; versions, kernel and pool features are in each `ENV.txt`), committed so the mocked suite can replay it through libzfseasy's parsers
(`tests/test_openzfs_formats.py`) without a pool. Nothing here is hand-written.

## Where they come from

`scenarios/test_openzfs_matrix.sh --captures` runs `captures.sh` in each VM, once per locale, on a throwaway
file-backed pool. Only the **C locale** pass (`LC_ALL=C LANG=C`) is kept here: the default-locale pass differs only in
non-deterministic order (clone lists, `zfs get all` rows, a random `pbkdf2salt`, a SIGPIPE race), and no
`zfs`/`zpool` message catalogue is installed on the VMs, so no message text differs.

Layout: `<version>/<SECTION>.txt` for each section, plus `M1-SUMMARY.tsv` (one parsed line per M1 resume token) and
`ENV.txt` (versions, locale, pool features).

| Section | Contents |
|---|---|
| M1 | `send -nv -t` of the resume token cut from a send, per send-flag set (128K/1M, lz4/off, plain/encrypted, full + incremental) |
| M2 | stale tokens: incremental source / target destroyed or recreated, sender renamed or moved, receiver base gone or replaced |
| M3_Z8 | `destroy -v -p` of single / batch / range / `-r` snapshots, with a hold, a clone, or a send in progress |
| M4 | `zfs list` with `createtxg,guid,userrefs,clones`, clone lists, holds, equal txgs |
| EP | encryption properties (`encryption`, `keystatus`, `encryptionroot`, `keyformat`, `keylocation`) |
| M5_Z1, M6 | raw / plain receive into pre-created or new datasets, `-F`, `-o`; `-L` mismatch between full and incremental |
| Z2, Z3, Z5, Z6, Z7 | ownership properties on raw receives, a partial new-dataset receive, `change-key`, `encryptionroot` of received copies, a raw copy with no key |
| Z4 | key unloaded: what still works, what fails, and the wording |
| M7 | `receive -F` / rollback / modified-head semantics (plan B-2b): a receiver ahead of the base, `rollback -r` vs local user properties, a modified head, a full stream onto a copy that has snapshots |
| M8 | the server receive argv (plan B-2a): `-o canmount/readonly/volmode` + `-x ...` (the first design) and the `-o`-only filesystem set / the canmount-free volume set (the second design), on plain and raw filesystems and volumes, `-p`, holds (`send -h` / `receive -h`), resume, `-F` onto an existing or modified copy, server-side operations on a readonly copy, created ancestors, a child under a received parent. Also the `HDR.*` records, below |
| M9 | `zfs version -j`, `zfs list -j`, `zfs get -j` (plan D8/S4a, OpenZFS 2.3+): values with newline, tab, `\xff`, quotes, backslashes, control bytes and non-ASCII; every source type; sort order, `-d`, `-t`, `-p`, `--json-int`; the `received` column; empty results and error shapes; `LC_ALL=C` vs `C.UTF-8`. On 2.1.5 and 2.2.2 only the fallback probe (`J*` records: `zfs version -j` exits 0 with plain text, `list -j`/`get -j` exit 2 with the usage text). `H*` records are the `-H` forms of the same objects, `00_name_*` which names `zfs create` accepts. The text files are not valid UTF-8 (a `0xff` byte): `load_section` decodes with `surrogateescape` |

**`M8/HDR.*` records** (stdout is `bytes=N` then one line of hex): the first 312 bytes of a `zfs send` stream, i.e. one
`struct dmu_replay_record` (the `DRR_BEGIN`: `drr_type u32`, `drr_payloadlen u32`, then `drr_magic u64`, `drr_versioninfo u64`,
`drr_creation_time u64`, `drr_type u32`, `drr_flags u32`, `drr_toguid u64`, `drr_fromguid u64`, `drr_toname[256]`). All
captures are from little-endian aarch64 VMs (`M8/C11.0_host`), so the integers are little-endian. A `-p` stream is a
compound stream: its first record is a stub (`versioninfo` 2, no dataset type) followed by `payloadlen` bytes of nvlist and a
`DRR_END`; the real `DRR_BEGIN` is the `HDR.*_inner` record (`payloadlen=N`, `inner_bytes=312`, then the hex of the 312 bytes
at offset `624+N`). `HDR.resume_*` come from `zfs send -t`. Creation times and guids differ per version; the flag fields do not.

M1 is trimmed: the `0_base`, `4_resume`, `5_after` and `6_snaps` records of each token case are left out (the summary
keeps the resume status). Every kept record has its exact bytes.

## Record format

```
##### SECTION <name> (<description>)  tag=C  <zfs version>
=== CASE <section>/<id>
--- cmd
<the command line, as run>
--- rc
<exit status (of the last command of a pipeline)>
--- pipestatus          only for pipelines: one status per stage
--- stdout
<exact stdout>
--- stderr
<exact stderr>
--- send.stderr         only for pipelines, and only when the sending side printed something
<stderr of the `zfs send` stage>
=== END
```

A resume token is the argument after `-t` in the `cmd` line. `tests/_openzfs_fixtures.py` parses a section file
into `{id: Record}` (`cmd`, `rc`, `stdout`, `stderr`, `pipestatus`, `send_stderr`, plus `.token`, `.stdout_lines`,
`.stderr_lines`); `load_m1_summary(version)` reads `M1-SUMMARY.tsv`. A capture writes every non-empty stdout/stderr with its final
newline, so the section files lose nothing against the per-record files (`<id>.{cmd,rc,stdout,stderr}`) the script
also leaves behind; an empty `send.stderr` has no marker.

## Regenerating

Needs the Multipass VMs (`zfsb-zfs21`, `zfsb-zfs22`, `zfsb-zfs24`) provisioned by the matrix script:

```
MATRIX_LOG_DIR=/some/dir scenarios/test_openzfs_matrix.sh --captures-only        # or --captures with the other tiers
```

The script leaves `<vm>.captures/{default,C}/` under `$MATRIX_LOG_DIR`. Copy each `C/<SECTION>.txt`,
`C/M1-SUMMARY.tsv` and `C/ENV.txt` to `tests/fixtures/openzfs/<2.1.5|2.2.2|2.4.1>/`, dropping the four M1 record kinds
named above if the total grows past ~1.5 MB. Then run `pytest tests/test_openzfs_formats.py`: a new OpenZFS release
that changes a wording or a flag set shows up as a failing, per-version test.
