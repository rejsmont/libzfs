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
