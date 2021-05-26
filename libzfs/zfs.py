from libzfs.types import Validate, ZFS, Dataset, Filesystem, Volume, Snapshot as Snap, Bookmark as Bookmk, \
    SnapshotRange, Property, Properties
from typing import Optional, Union, Dict, Iterable, Callable

import subprocess
import shutil

ZFS_BIN = shutil.which('zfs')

Snapshots = Union[Snap, SnapshotRange]
SnapshotList = Union[Snapshots, Iterable[Snapshots]]
Datasets = Union[Filesystem, Volume, Dataset, Snap]
DatasetList = Union[Datasets, Iterable[Datasets]]
ZFSList = Union[ZFS, Iterable[ZFS]]
StringList = Union[str, Iterable[str]]
Sort = Union[str, Iterable[str], Dict[str, bool]]


class Command:

    @classmethod
    def _exec(cls, cmd):
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        errors = []
        while True:
            output = cls._exec_capture(process, errors)
            if output is False:
                break

    @classmethod
    def _exec_out(cls, cmd):
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        errors = []
        while True:
            output = cls._exec_capture(process, errors)
            if output is False:
                break
            elif output != '':
                yield output

    @staticmethod
    def _exec_capture(process, errors):
        stdout = process.stdout.readline()
        stderr = process.stderr.readline()
        rc = process.poll()
        if stderr:
            error = stderr.strip()
            errors.append(error[0].upper() + error[1:])
        if stdout != '':
            return stdout.strip()
        if stdout == '' and stderr == '' and rc is not None:
            if rc != 0:
                raise Exception('\n'.join(errors))
            return False
        return None


class SListArgument:

    @staticmethod
    def _slist_to_list(slist: StringList, separator: str = ',', validator: Optional[Callable[[str], None]] = None):
        if not slist:
            slist = []
        elif isinstance(slist, str):
            slist = str(slist).split(separator)
        if validator:
            for s in slist:
                validator(s)
        return list(slist)

    @classmethod
    def _slist_to_str(cls, slist: StringList, separator: str = ',', validator: Optional[Callable[[str], None]] = None):
        return separator.join([str(s) for s in cls._slist_to_list(slist, separator, validator)])


class DsListArgument:

    @staticmethod
    def _dslist_to_list(dslist: DatasetList):
        if not dslist:
            dslist = []
        elif not isinstance(dslist, Iterable):
            dslist = [dslist]
        for ds in dslist:
            if not isinstance(ds, (Filesystem, Volume, Dataset, Snap)):
                raise ValueError('Expected Filesystem, Volume, Dataset or Snapshot, got '
                                 + type(ds).__name__ + ' instead')
        return list(dslist)

    @classmethod
    def _dslist_to_str(cls, dslist: DatasetList, separator: str = ','):
        return separator.join([str(s) for s in cls._dslist_to_list(dslist)])


class ZListArgument:

    @staticmethod
    def _zlist_to_list(zlist: ZFSList):
        if not zlist:
            zlist = []
        elif not isinstance(zlist, Iterable):
            zlist = [zlist]
        for ds in zlist:
            if not isinstance(ds, ZFS):
                raise ValueError('Expected Filesystem, Volume, Dataset, Snapshot or Bookmark, got '
                                 + type(ds).__name__ + ' instead')
        return list(zlist)

    @classmethod
    def _zlist_to_str(cls, dslist: DatasetList, separator: str = ','):
        return separator.join([str(s) for s in cls._zlist_to_list(dslist)])


class List(Command, SListArgument, DsListArgument):

    def __call__(self, *args, **kwargs):
        """List ZFS datasets.

        Keyword arguments:
            root: Dataset -- specify a root dataset to list from (default None)
            dstype: str -- types of datasets to list (default None)
            recursive: bool -- whether to recursively list root dataset children (default False)
            depth: int -- child dataset depth to list (default 0)
            properties: str | [str] -- properties to include in the result (default None)
            sort: str | [str] | {str, bool} -- result sorting order (default None)
            asc: bool -- whether to sort in ascending order (default True)
            lazy: bool -- whether to return a generator instead of a list (default False)

        Returns:
            An Iterable containing ZFS datasets -- a Generator if lazy is True, List otherwise
        """

        return self._list(*args, **kwargs)

    @classmethod
    def _list(cls, roots: Optional[DatasetList] = None, types: Optional[str] = None, recursive: bool = False,
              depth: int = 0, properties: Optional[StringList] = None, sort: Optional[Sort] = None,
              asc: bool = True, lazy=False) -> Iterable[ZFS]:

        cmd = [ZFS_BIN, 'list', '-H']

        if not recursive:
            cmd += ['-d', '1']
        elif depth > 0:
            cmd += ['-d', str(depth)]

        if types:
            cmd += ['-t', cls._slist_to_str(types, validator=Validate.type)]

        if isinstance(sort, str):
            Validate.attribute(sort)
            cmd += ['-s' if asc else 'S', sort]
        elif isinstance(sort, Iterable):
            for s in sort:
                Validate.attribute(s)
                cmd += ['-s', s]
        elif isinstance(sort, dict):
            for k, s in sort.items():
                Validate.attribute(k)
                cmd += ['-s' if s else 'S', k]

        if properties is None:
            properties = ['name', 'type']
        else:
            properties = cls._slist_to_list(properties, validator=Validate.attribute)
            properties = ['name', 'type'] + properties
        cmd += ['-o', ','.join(properties)]

        if roots:
            cmd += ['-r', cls._dslist_to_str(roots)]

        result = (cls._line_to_object(line, properties) for line in cls._exec_out(cmd))

        if lazy:
            return (o for o in result if o is not None)
        else:
            return [o for o in result if o is not None]

    @staticmethod
    def _line_to_object(line: str, properties: Iterable):
        zfs_info = line.split()
        zfs_info = [i if not i == '-' else None for i in zfs_info]
        name, dstype = zfs_info[0:2]
        if len(zfs_info) > 2:
            props = dict(zip(properties[2:], zfs_info[2:]))
        else:
            props = None
        return ZFS.from_name(name, dstype, props)


class Create(Command):

    def __call__(self, *args, **kwargs):
        return self._create(*args, **kwargs)

    @classmethod
    def _create(cls, ds: str, *args, **kwargs) -> Union[Filesystem, Volume]:
        if (args and isinstance(args[0], str)) or ('size' in kwargs):
            return cls.volume(ds, *args, **kwargs)
        else:
            return cls.filesystem(ds, *args, **kwargs)

    @classmethod
    def filesystem(cls, ds: str, properties: Optional[Properties] = None, mount=True, parents=False) -> Filesystem:

        filesystem = Filesystem(ds, properties)
        cmd = [ZFS_BIN, 'create']
        if parents:
            cmd += ['-p']
        if not mount:
            cmd += ['-u']
        if properties:
            for k, v in filesystem.properties:
                cmd += ['-o', k + '=' + str(v)]
        cmd += [str(filesystem)]
        cls._exec(cmd)

        return filesystem

    @classmethod
    def volume(cls, ds: str, size: str, properties: Optional[Properties] = None, sparse=False,
               parents=False) -> Volume:

        volume = Volume(ds, properties)
        cmd = [ZFS_BIN, 'create', '-V', size]
        if parents:
            cmd += ['-p']
        if not sparse:
            cmd += ['-s']
        if properties:
            for k, v in volume.properties:
                cmd += ['-o', k + '=' + str(v)]
        cmd += [str(volume)]
        cls._exec(cmd)

        return volume


class Snapshot(Command):

    def __call__(self, *args, **kwargs):
        return self._snapshot(*args, **kwargs)

    @classmethod
    def _snapshot(cls, ds: Dataset, name: str, recursive: bool = False,
                  properties: Optional[Properties] = None) -> Snap:

        snapshot = Snap(ds, name, properties)
        cmd = [ZFS_BIN, 'snapshot']
        if recursive:
            cmd += ['-r']
        if properties:
            for k, v in snapshot.properties:
                cmd += ['-o', k + '=' + str(v)]
        cmd += [str(snapshot)]
        cls._exec(cmd)

        return snapshot


class Bookmark(Command):

    def __call__(self, *args, **kwargs):
        return self._bookmark(*args, **kwargs)

    @classmethod
    def _bookmark(cls, ds: Union[Bookmk, Snap], name: str) -> Bookmk:

        if not isinstance(ds, (Bookmk, Snap)):
            raise ValueError('Expected Bookmark or Snapshot, got ' + type(ds).__name__ + ' instead')

        bookmark = Bookmk(ds.dataset, name)
        cmd = [ZFS_BIN, 'bookmark', str(bookmark)]
        cls._exec(cmd)

        return bookmark


class Destroy(Command):

    def __call__(self, *args, **kwargs):
        return self._destroy(*args, **kwargs)

    @classmethod
    def _destroy(cls, ds: Union[Datasets, Bookmark, SnapshotList], *args, **kwargs):
        if isinstance(ds, Dataset):
            return cls.dataset(ds, *args, **kwargs)
        elif isinstance(ds, Snapshot) or isinstance(ds, SnapshotRange) or isinstance(ds, Iterable):
            return cls.snapshots(ds, *args, **kwargs)
        elif isinstance(ds, Bookmark):
            # noinspection PyArgumentList
            cls.bookmark(ds, *args, **kwargs)
        else:
            raise ValueError('Expected Filesystem, Volume, Snapshot or Bookmark, got ' + type(ds).__name__ + ' instead')

    @staticmethod
    def _base(destroy: bool = False, recursive: bool = False, clones: bool = False) -> Iterable[str]:
        cmd = [ZFS_BIN, 'destroy', '-v', '-p']
        if not destroy:
            cmd += ['-n']
        if recursive:
            cmd += ['-R'] if clones else ['-r']
        elif clones:
            raise ValueError('Clones can only be set to True if recursive is also set to True')
        return cmd

    @classmethod
    def dataset(cls, dataset: Dataset, destroy: bool = False, recursive: bool = False, clones: bool = False,
                force: bool = False, lazy: bool = False) -> Iterable[str]:

        if not isinstance(dataset, Dataset):
            raise ValueError('Expected Filesystem or Volume, got ' + type(dataset).__name__ + ' instead')

        cmd = cls._base(destroy, recursive, clones)
        if force:
            cmd += ['-f']
        cmd += [str(dataset)]
        result = (line.strip().replace('destroy\t', '') for line in cls._exec_out(cmd))

        if lazy:
            return (o for o in result)
        else:
            return [o for o in result]

    @classmethod
    def snapshots(cls, snapshots: Union[Snapshots, Iterable[Snapshots]], destroy: bool = False, recursive: bool = False,
                  clones: bool = False, force: bool = False, lazy: bool = False) -> Iterable[str]:

        ds, snaps = None, ''
        if not isinstance(snapshots, Iterable):
            snapshots = [snapshots]
        for snap in snapshots:
            if not isinstance(snap, (Snapshot, SnapshotRange)):
                raise ValueError('Expected Snapshot or SnapshotRange, got ' + type(snap).__name__ + ' instead')
            if ds is None:
                ds = snap.dataset
            elif str(snap.dataset) != str(ds):
                raise ValueError('Snapshots must come from the same dataset')
            snaps += ',' + snap.short if snaps else snap.short
        snaps = ds.name + '@' + snaps

        cmd = cls._base(destroy, recursive, clones)
        if force:
            cmd += ['-d']
        cmd += [snaps]
        result = (line.strip().replace('destroy\t', '') for line in cls._exec_out(cmd))

        if lazy:
            return (o for o in result)
        else:
            return [o for o in result]

    @classmethod
    def bookmark(cls, bookmark: Bookmk) -> None:

        if not isinstance(bookmark, Bookmk):
            raise ValueError('Expected Bookmark, got ' + type(bookmark).__name__ + ' instead')

        cmd = [ZFS_BIN, 'destroy', bookmark]
        cls._exec(cmd)


class Rename(Command):

    def __call__(self, *args, **kwargs):
        return self._rename(*args, **kwargs)

    @classmethod
    def _rename(cls, ds: Datasets, name: str, **kwargs):
        if isinstance(ds, Filesystem):
            return cls.filesystem(ds, name, **kwargs)
        elif isinstance(ds, Volume):
            return cls.volume(ds, name, **kwargs)
        elif isinstance(ds, Snapshot):
            return cls.snapshot(ds, name, **kwargs)
        else:
            raise ValueError('Expected Filesystem, Volume or Snapshot, got ' + type(ds).__name__ + ' instead')

    @staticmethod
    def _base(force: bool = False, parents: bool = False):
        cmd = [ZFS_BIN, 'rename']
        if force:
            cmd += ['-f']
        if parents:
            cmd += ['-p']
        return cmd

    @classmethod
    def filesystem(cls, dataset: Filesystem, name: str, mount: bool = True, force: bool = False,
                   parents: bool = False) -> Filesystem:

        new = Filesystem(name, dict(dataset.properties))
        cmd = cls._base(force, parents)
        if not mount:
            cmd += ['-u']
        cmd += [str(dataset), str(new)]
        cls._exec(cmd)

        return new

    @classmethod
    def volume(cls, dataset: Volume, name: str, force: bool = False, parents: bool = False) -> Volume:

        new = Volume(name, dict(dataset.properties))
        cmd = cls._base(force, parents)
        cmd += [str(dataset), str(new)]
        cls._exec(cmd)

        return new

    @classmethod
    def snapshot(cls, snapshot: Snap, name: str, force: bool = False, parents: bool = False) -> Snap:
        if '@' in name.strip('@'):
            new_snap = Snap.from_name(name)
            name = new_snap.short
            if str(new_snap.dataset) != str(snapshot.dataset):
                raise ValueError('Snapshots can only be renamed within the parent dataset')
        new = Snap(snapshot.ds, name, dict(snapshot.properties))
        cmd = cls._base(force, parents)
        cmd += [str(snapshot), str(new)]
        cls._exec(cmd)

        return new


class Allow(Command, SListArgument):

    def __call__(self, *args, **kwargs):
        return self._allow(True, *args, **kwargs)

    def create(self, *args, **kwargs):
        return self._create(True, *args, **kwargs)

    def set(self, *args, **kwargs):
        return self._set(True, *args, **kwargs)

    @classmethod
    def _allow(cls, allow: bool, ds: Dataset, permissions: StringList, users: Optional[StringList] = None,
               groups: Optional[StringList] = None, everyone: bool = False, dataset: bool = True, children: bool = True,
               recursive: bool = False) -> None:

        cmd = cls._base(allow, None, recursive)
        if dataset and not children:
            cmd += ['-l']
        elif children and not dataset:
            cmd += ['-d']
        elif not children and not dataset:
            raise ValueError('At least one of dataset or children needs to be True')
        if everyone:
            if users or groups:
                raise ValueError('Everyone and users or groups cannot be specified simultaneously')
            cmd += ['-e', users]
        else:
            if users:
                cmd += ['-u', cls._slist_to_str(users)]
            if groups:
                cmd += ['-g', cls._slist_to_str(groups)]
        cmd += [cls._slist_to_str(permissions), ds]
        cls._exec(cmd)

    @classmethod
    def _create(cls, allow: bool, ds: Dataset, permissions: StringList, recursive: bool = False) -> None:

        cmd = cls._base(allow, '-c', recursive)
        cmd += [cls._slist_to_str(permissions), ds]
        cls._exec(cmd)

    @classmethod
    def _set(cls, allow: bool, ds: Dataset, name: str, permissions: StringList, recursive: bool = False) -> None:
        try:
            Validate.zfsname(name.strip('@'))
            valid = True
        except ValueError:
            valid = False
        if not (name.startswith('@') and valid):
            raise ValueError(name + ' is not a valid ZFS permission set name')

        cmd = cls._base(allow, '-s', recursive)
        cmd += [name, cls._slist_to_str(permissions), ds]
        cls._exec(cmd)

    @staticmethod
    def _base(allow: bool, param: Optional[str] = None, recursive: bool = False) -> Iterable[str]:
        extra = [param] if param else []
        if allow:
            cmd = [ZFS_BIN, 'allow'] + extra
            if recursive:
                raise TypeError('_zfs_allow_base() got an unexpected keyword argument \'recursive\'')
        else:
            cmd = [ZFS_BIN, 'unallow'] + extra
            if recursive:
                cmd += ['-r']

        return cmd


class UnAllow(Allow):

    def __call__(self, *args, **kwargs):
        return self._allow(False, *args, **kwargs)

    def create(self, *args, **kwargs):
        return self._create(False, *args, **kwargs)

    def set(self, *args, **kwargs):
        return self._set(False, *args, **kwargs)


class Clone(Command):

    def __call__(self, *args, **kwargs):
        return self._clone(*args, **kwargs)

    @classmethod
    def _clone(cls, snapshot: Snapshot, ds: str, properties: Optional[Properties] = None, parents=False) -> Dataset:

        dataset = Dataset(ds, properties)
        cmd = [ZFS_BIN, 'clone']
        if parents:
            cmd += ['-p']
        if properties:
            for k, v in dataset.properties:
                cmd += ['-o', k + '=' + str(v)]
        cmd += [str(snapshot), str(dataset)]
        cls._exec(cmd)

        return dataset


class Get(Command, SListArgument, ZListArgument):

    def __call__(self, *args, **kwargs):
        return self._get(*args, **kwargs)

    @classmethod
    def _get(cls, ds: Optional[DatasetList] = None, properties: Optional[StringList] = 'all',
             types: Optional[StringList] = 'all', sources: Optional[StringList] = 'all', recursive: bool = False,
             depth: int = 0, lazy=False) -> Iterable[ZFS]:

        cmd = [ZFS_BIN, 'get', '-H', '-o', 'all']
        sources = cls._slist_to_list(sources, validator=Validate.source)

        if not recursive and ds:
            cmd += ['-d', '1']
        elif depth > 0:
            cmd += ['-d', str(depth)]

        cmd += ['-t', cls._slist_to_str(types, validator=Validate.type)]

        properties = cls._slist_to_list(properties, validator=Validate.attribute)
        if 'all' in properties:
            properties = ['all']
        else:
            properties = ['name', 'type'] + properties
        cmd += [','.join(properties)]

        if ds:
            cmd += [cls._zlist_to_str(ds)]

        result = cls._lines_to_objects((line for line in cls._exec_out(cmd)), sources)

        if lazy:
            return (o for o in result if o is not None)
        else:
            return [o for o in result if o is not None]

    @staticmethod
    def _lines_to_objects(lines, sources) -> Iterable[Datasets]:
        dsname, properties = '', {}
        for line in lines:
            name, prop, value, received, source = line.split('\t')
            if not dsname:
                dsname = name
            elif name != dsname:
                properties.pop('name', None)
                dstype = properties.pop('type', None)
                if name and dstype:
                    yield ZFS.from_name(dsname, dstype, properties)
                    dsname, properties = '', {}
            if prop == 'type':
                properties[prop] = value
            elif 'all' in sources or source in sources:
                properties[prop] = Property(value, source, received)
