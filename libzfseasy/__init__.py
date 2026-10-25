from libzfseasy.types import Bookmark as _Bookmark
from libzfseasy.zfs import (
    ListCommand, CreateCommand, SnapshotCommand, BookmarkCommand, DestroyCommand, RenameCommand,
    AllowCommand, UnAllowCommand, CloneCommand, GetCommand, SetCommand, InheritCommand,
    SendCommand, ReceiveCommand, ChangeKeyCommand, LoadKeyCommand, UnLoadKeyCommand,
    MountCommand, UnMountCommand
)

# Create command instances
list = ListCommand()
create = CreateCommand()
snapshot = SnapshotCommand()
bookmark = BookmarkCommand()
destroy = DestroyCommand()
rename = RenameCommand()
allow = AllowCommand()
unallow = UnAllowCommand()
clone = CloneCommand()
get = GetCommand()
set = SetCommand()
inherit = InheritCommand()
send = SendCommand()
receive = ReceiveCommand()
recv = ReceiveCommand()
load_key = LoadKeyCommand()
unload_key = UnLoadKeyCommand()
change_key = ChangeKeyCommand()
mount = MountCommand()
unmount = UnMountCommand()

# diff
# groupspace
# hold
# jail
# program
# project
# projectspace
# promote
# redact
# release
# rollback
# share
# unjail
# upgrade
# userspace
# wait

def _reason(error: Exception) -> str:
    """The lowercased reason of a zfs failure: the text after the last `': ` of its message (zfs writes
    `cannot open '<name>': <reason>`), or the whole message when there is no such separator. The name zfs quotes is
    chosen by whoever created the object, so it must never be searched for a reason."""
    message = str(error).lower()
    head, sep, reason = message.rpartition("': ")
    return reason if sep else message


def exists(ds):
    """Whether a dataset, snapshot or bookmark exists.

    A Bookmark is checked with `zfs get -H -o value type <bookmark>`, because the `list` roots path refuses a
    Bookmark; every other argument goes through `zfs list`.

    Returns:
        bool -- False when zfs says the object does not exist (matched on zfs's reason, not on the quoted name);
            any other failure is raised
    """
    if isinstance(ds, _Bookmark):
        try:
            return get.value(ds, 'type') == 'bookmark'
        except Exception as e:
            # A missing bookmark on an existing dataset reads "no such pool or dataset"; a missing parent or pool
            # reads "dataset does not exist" (OpenZFS 2.1.5, 2.2.2, 2.4.1, under LC_ALL=C).
            reason = _reason(e)
            if "does not exist" in reason or "no such pool or dataset" in reason:
                return False
            raise
    try:
        result = list(roots=ds)
    except Exception as e:
        if "dataset does not exist" in _reason(e):
            return False
        raise e
    return len(result) == 1
