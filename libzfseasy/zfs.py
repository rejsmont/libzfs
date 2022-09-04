from libzfseasy.types import Validate, ZFS, Dataset, Filesystem, Volume, Snapshot as Snap, Bookmark as Bookmk, \
     SnapshotRange, Property, Properties
from typing import Optional, Union, Dict, Iterable, Callable

import io
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


class PropertyCommand:

    @staticmethod
    def _get_props(properties: Properties, flag: bool = False):
        cmd = []
        for k, v in properties:
            if flag:
                cmd += ['-o', k + '=' + str(v)]
            else:
                cmd += [k + '=' + str(v)]
        return cmd


class StringListArgument:

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


class DatasetListArgument:

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


class ZFSListArgument:

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


class List(Command, StringListArgument, DatasetListArgument):

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
        """Create ZFS filesystem or volume.

        Keyword arguments:
            ds: str -- dataset or volume name
            properties: Properties -- properties to set on the created dataset / volume (default None)

        Returns:
            A ZFS Filesystem or Volume object
        """

        return self._create(*args, **kwargs)

    @classmethod
    def _create(cls, ds: str, *args, **kwargs) -> Union[Filesystem, Volume]:
        if (args and isinstance(args[0], str)) or ('size' in kwargs):
            return cls.volume(ds, *args, **kwargs)
        else:
            return cls.filesystem(ds, *args, **kwargs)

    @classmethod
    def filesystem(cls, ds: str, properties: Optional[Properties] = None, mount=True, parents=False) -> Filesystem:
        """Create ZFS filesystem.

        Keyword arguments:
            ds: str -- dataset name
            properties: Properties -- properties to set on the created dataset (default None)
            mount: bool -- should the created dataset be mounted (default True)
            parents: bool -- whether to create parent datasets if needed (default False)

        Returns:
            A ZFS Filesystem object
        """

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
        
        """Create ZFS volume.

        Keyword arguments:
            ds: str -- dataset or volume name
            size: str -- volume size
            properties: Properties -- properties to set on the created volume (default None)
            sparse: bool -- should the created volume be sparse (default False)
            parents: bool -- whether to create parent datasets if needed (default False)

        Returns:
            A ZFS Volume object
        """

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


class Allow(Command, StringListArgument):

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


class Get(Command, StringListArgument, ZFSListArgument):

    def __call__(self, *args, **kwargs):
        return self._get(*args, **kwargs)

    @classmethod
    def _get(cls, ds: Optional[DatasetList] = None, properties: Optional[StringList] = 'all',
             types: Optional[StringList] = 'all', sources: Optional[StringList] = 'all', recursive: bool = False,
             depth: int = 0, lazy=False) -> Iterable[ZFS]:

        sources = cls._slist_to_list(sources, validator=Validate.source)
        types = cls._slist_to_str(types, validator=Validate.type)
        properties = cls._slist_to_list(properties, validator=Validate.attribute)
        datasets = cls._zlist_to_str(ds)
        cmd = [ZFS_BIN, 'get', '-H', '-o', 'all'] + \
            cls._get_options(recursive=recursive, depth=depth, types=types, properties=properties, datasets=datasets)
        result = cls._lines_to_objects((line for line in cls._exec_out(cmd)), sources)

        if lazy:
            return (o for o in result if o is not None)
        else:
            return [o for o in result if o is not None]

    @staticmethod
    def _get_options(**kwargs):
        cmd = []
        if kwargs.get('recursive', False):
            cmd += ['-r']

        depth = kwargs.get('depth', 0)
        if depth > 0:
            cmd += ['-d', str(depth)]
        cmd += ['-t', kwargs.get('types')]

        properties = kwargs.get('properties')
        if 'all' in properties:
            properties = ['all']
        else:
            properties = ['name', 'type'] + properties
        cmd += [','.join(properties)]

        datasets = kwargs.get('datasets', False)
        if datasets:
            cmd += [datasets]

        return cmd

    @staticmethod
    def _lines_to_objects(lines, sources) -> Iterable[Datasets]:
        name, dsname, properties = '', '', {}
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

        properties.pop('name', None)
        dstype = properties.pop('type', None)
        if name and dstype:
            yield ZFS.from_name(dsname, dstype, properties)


class Set(Command):

    def __call__(self, *args, **kwargs):
        return self._set(*args, **kwargs)

    @classmethod
    def _set(cls, ds: Datasets, properties: Properties) -> Datasets:

        ds.update(properties)
        cmd = [ZFS_BIN, 'set'] + cls._get_props(properties) + [str(ds)]
        cls._exec(cmd)

        return ds

    @staticmethod
    def _get_props(properties: Properties, flag: bool = False):
        cmd = []
        for k, v in properties:
            if flag:
                cmd += ['-o', k + '=' + str(v)]
            else:
                cmd += [k + '=' + str(v)]
        return cmd


class Inherit(Command):

    def __call__(self, *args, **kwargs):
        return self._inherit(*args, **kwargs)

    @classmethod
    def _inherit(cls, ds: Datasets, prop: str, recursive: bool = False, received: bool = False) -> Datasets:

        ds.update({prop: None})
        cmd = [ZFS_BIN, 'inherit', prop] + cls._get_options(recursive=recursive, received=received) + [str(ds)]
        cls._exec(cmd)

        return ds

    @staticmethod
    def _get_options(**kwargs):
        cmd = []
        if kwargs.get('recursive', False):
            cmd += ['-r']
        if kwargs.get('received', False):
            cmd += ['-S']
        return cmd


class Send(Command):

    def __call__(self, *args, **kwargs):
        return self._send(*args, **kwargs)

    @classmethod
    def _send(cls, ds: Datasets, *args, **kwargs) -> Optional[io.BufferedReader]:
        if isinstance(ds, Dataset):
            return cls.dataset(ds, *args, **kwargs)
        elif isinstance(ds, Snapshot):
            return cls.snapshot(ds, *args, **kwargs)

    @classmethod
    def snapshot(cls, ds: Snap, since: Optional[Snap] = None, intermediate: bool = False,
                 replicate: bool = False, holds: bool = False, properties: bool = False, backup: bool = False,
                 raw: bool = False, compressed: bool = False, embed: bool = False, large_blocks: bool = False,
                 skip_missing: bool = False) -> Optional[io.BufferedReader]:

        """Generate a send stream for a given ZFS snapshot

        Keyword arguments:
            ds: Snap -- source ZFS snapshot
            since: Snap -- generate incremental stream from the specified snapshot (default None)
            intermediate: bool -- generate intermediate stream (default False)
            replicate: bool -- replicate the stream (default False)
            holds: bool -- include hold tags (default False)
            properties: bool -- include properties (default False)
            backup: bool -- include backup stream header (default False)
            raw: bool -- for encrypted datasets, send data exactly as it exists on disk (default False)
            compressed: bool -- generate a more compact stream by using dataset compression (default False)
            embed: bool -- generate a more compact stream by using the embedded_data pool feature (default False)
            large_blocks: bool -- generate a stream which may contain blocks larger than 128KB (default False)
            skip_missing: bool -- skip over missing snapshots (default False)

        Returns:
            BufferedReader
        """

        cmd = [ZFS_BIN, 'send'] + \
            cls._get_options(since=since, intermediate=intermediate, replicate=replicate, holds=holds,
                             properties=properties, backup=backup, raw=raw, compressed=compressed, embed=embed,
                             large_blocks=large_blocks, skip_missing=skip_missing) + [str(ds)]

        return cls._exec_stream(cmd)

    @classmethod
    def dataset(cls, ds: Dataset, since: Optional[Snap] = None, raw: bool = False, compressed: bool = False,
                embed: bool = False, large_blocks: bool = False) -> Optional[io.BufferedReader]:

        """Generate a send stream for a given ZFS dataset

        Keyword arguments:
            ds: Dataset -- source ZFS dataset (filesystem or volume)
            since: Snap -- generate incremental stream from the specified snapshot (default None)
            raw: bool -- for encrypted datasets, send data exactly as it exists on disk (default False)
            compressed: bool -- generate a more compact stream by using dataset compression (default False)
            embed: bool -- generate a more compact stream by using the embedded_data pool feature (default False)
            large_blocks: bool -- generate a stream which may contain blocks larger than 128KB (default False)

        Returns:
            BufferedReader
        """

        cmd = [ZFS_BIN, 'send'] + \
            cls._get_options(since=since, raw=raw, compressed=compressed, embed=embed,
                             large_blocks=large_blocks) + [str(ds)]

        return cls._exec_stream(cmd)

    @classmethod
    def redact(cls, ds: Snap, redact: Bookmk, since: Union[Snap, Bookmk, None] = None,
               properties: bool = False, compressed: bool = False, embed: bool = False,
               large_blocks: bool = False) -> Optional[io.BufferedReader]:

        """Generate a redacted send stream

        Keyword arguments:
            ds: Snap -- source ZFS snapshot
            redact: Bookmk -- bookmark containing the redaction list of blocks to exclude from the stream
            since: Snap -- generate incremental stream from the specified snapshot (default None)
            properties: bool -- include dataset properties in the send stream (default False)
            compressed: bool -- generate a more compact stream by using dataset compression (default False)
            embed: bool -- generate a more compact stream by using the embedded_data pool feature (default False)
            large_blocks: bool -- generate a stream which may contain blocks larger than 128KB (default False)

        Returns:
            BufferedReader
        """

        cmd = [ZFS_BIN, 'send', '--redact', str(redact)] + \
            cls._get_options(since=since, properties=properties, compressed=compressed, embed=embed,
                             large_blocks=large_blocks) + [str(ds)]

        return cls._exec_stream(cmd)

    @classmethod
    def resume(cls, token: str, embed: bool = False) -> Optional[io.BufferedReader]:
        """Creates a send stream which resumes an interrupted receive

         Keyword arguments:
            token: str -- resume token generated by the receiving side
            embed: bool -- generate a more compact stream by using the embedded_data pool feature (default False)

        Returns:
            BufferedReader
        """
        cmd = [ZFS_BIN, 'send'] + cls._get_options(embed=embed) + ['-t', token]
        return cls._exec_stream(cmd)

    @classmethod
    def partial(cls, ds: Dataset, since: Optional[Snapshot] = None) -> Optional[io.BufferedReader]:
        """Generate a send stream from a dataset that has been partially received

        Keyword arguments:
            ds: Filesystem -- source ZFS filesystem
            since: Snapshot -- generate incremental stream from the specified snapshot (default None)

        Returns:
            BufferedReader
        """

        cmd = [ZFS_BIN, 'send'] + cls._get_options(since=since) + ['-S', str(ds)]

        return cls._exec_stream(cmd)

    @staticmethod
    def _get_options(**kwargs):
        cmd = []
        if kwargs.get('replicate', False):
            cmd += ['-R']
        if kwargs.get('holds', False):
            cmd += ['-h']
        if kwargs.get('properties', False):
            cmd += ['-p']
        if kwargs.get('backup', False):
            cmd += ['-b']
        if kwargs.get('raw', False):
            cmd += ['-w']
        if kwargs.get('compressed', False):
            cmd += ['-c']
        if kwargs.get('embed', False):
            cmd += ['-e']
        if kwargs.get('large_blocks', False):
            cmd += ['-L']
        if kwargs.get('skip_missing', False):
            if kwargs.get('replicate', False):
                cmd += ['-s']
            else:
                raise ValueError('skip_missing can only be specified together with replicate')
        since = kwargs.get('since', False)
        if since:
            if not isinstance(since, Snap):
                raise ValueError('Expected Snapshot, got ' + type(since).__name__ + ' instead')
            elif kwargs.get('intermediate', False):
                cmd += ['-I', str(since)]
            else:
                cmd += ['-i', str(since)]

        return cmd

    @classmethod
    def _exec_stream(cls, cmd) -> Optional[io.BufferedReader]:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        errors = []
        while True:
            output = cls._exec_capture_bin(process, errors)
            if output is False:
                break
            elif output is not None:
                return output
        return None

    @staticmethod
    def _exec_capture_bin(process, errors) -> Union[bool, io.BufferedReader, None]:
        stdout = process.stdout.peek(8)
        stderr = io.TextIOWrapper(process.stderr).readline()
        rc = process.poll()
        if stderr:
            error = stderr.strip()
            errors.append(error[0].upper() + error[1:])
        if len(stdout):
            return process.stdout
        elif stderr != '' and rc is not None:
            if rc != 0:
                raise Exception('\n'.join(errors))
            return False
        else:
            return None


class Receive(Command, StringListArgument):

    def __call__(self, *args, **kwargs):
        return self._receive(*args, **kwargs)

    @classmethod
    def _receive(cls, ds: Datasets, *args, **kwargs) -> Optional[io.BufferedWriter]:
        if isinstance(ds, Filesystem):
            return cls.filesystem(ds, *args, **kwargs)
        else:
            return cls.dataset(ds, *args, **kwargs)

    @classmethod
    def filesystem(cls, ds: Filesystem, props: Optional[Properties] = None, reset: Optional[StringList] = None,
                   origin: Optional[Snap] = None, force: bool = False, holds: bool = True, unmount: bool = False,
                   save: bool = False, mount: bool = True, ignore_first: bool = False,
                   ignore_all: bool = False) -> Optional[io.BufferedWriter]:

        """Receive ZFS filesystem stream

        Keyword arguments:
            ds: Filesystem -- target ZFS filesystem
            props: Properties -- properties to set on the target filesystem (default None)
            reset: Properties -- properties to reset on the target filesystem (default None)
            origin: Snap -- forces the stream to be received as a clone of the given snapshot (default None)
            force: bool -- force discarding changes or incompatible snapshots from the target filesystem (default False)
            holds: bool -- receive holds (default True)
            unmount: bool -- unmount the target filesystem during transfer (default False)
            save: bool -- save state token for resuming the transfer (default False)
            mount: bool -- whether to mount the received filesystem (default True)
            ignore_first: bool -- discard the first part of snapshot filesystem name (default False)
            ignore_all: bool -- discard all but the last part of snapshot filesystem name (default False)

        Returns:
            BufferedWriter
        """
        cmd = [ZFS_BIN, 'receive'] + \
            cls._get_options(props=props, reset=reset, origin=origin, force=force, holds=holds, unmount=unmount,
                             save=save, mount=mount, ignore_first=ignore_first, ignore_all=ignore_all) + [str(ds)]

        return cls._exec_stream_in(cmd)

    @classmethod
    def dataset(cls, ds: Datasets, props: Optional[Properties] = None, reset: Optional[StringList] = None,
                origin: Optional[Snap] = None, force: bool = False, holds: bool = True, unmount: bool = False,
                save: bool = False, mount: bool = True) -> Optional[io.BufferedWriter]:

        """Receive ZFS filesystem stream

        Keyword arguments:
            ds: Filesystem -- target ZFS filesystem
            props: Properties -- properties to set on the target filesystem (default None)
            reset: Properties -- properties to reset on the target filesystem (default None)
            origin: Snap -- forces the stream to be received as a clone of the given snapshot (default None)
            force: bool -- force discarding changes or incompatible snapshots from the target filesystem (default False)
            holds: bool -- receive holds (default True)
            unmount: bool -- unmount the target filesystem during transfer (default False)
            save: bool -- save state token for resuming the transfer (default False)
            mount: bool -- whether to mount the received filesystem (default True)

        Returns:
            BufferedWriter
        """
        cmd = [ZFS_BIN, 'receive'] + \
            cls._get_options(props=props, reset=reset, origin=origin, force=force, holds=holds, unmount=unmount,
                             save=save, mount=mount) + [str(ds)]

        return cls._exec_stream_in(cmd)

    @classmethod
    def abort(cls, ds: Datasets) -> None:
        """Abort an interrupted ZFS receive operation, deleting its saved partially received state

        Keyword arguments:
            ds: Filesystem -- target ZFS filesystem

        Returns:
            None
        """

        cmd = [ZFS_BIN, '-A'] + [str(ds)]

        return cls._exec(cmd)

    @classmethod
    def _get_options(cls, **kwargs):
        cmd = []
        origin = kwargs.get('origin', None)
        if origin:
            cmd += ['-o', 'origin=' + str(origin)]
        props = kwargs.get('props', None)
        if props:
            cmd += cls._get_props(props, True)
        reset = kwargs.get('reset', None)
        if reset:
            cmd += ['-x ' + s for s in cls._slist_to_list(reset)]
        if kwargs.get('force', False):
            cmd += ['-F']
        if not kwargs.get('holds', True):
            cmd += ['-h']
        if kwargs.get('unmount', False):
            cmd += ['-M']
        if kwargs.get('save', False):
            cmd += ['-s']
        if not kwargs.get('mount', True):
            cmd += ['-u']
        if kwargs.get('ignore_first', False):
            cmd += ['-d']
        if kwargs.get('ignore_sll', False):
            cmd += ['-e']

        return cmd

    @staticmethod
    def _get_props(properties: Properties, flag: bool = False):
        cmd = []
        for k, v in properties:
            if flag:
                cmd += ['-o', k + '=' + str(v)]
            else:
                cmd += [k + '=' + str(v)]
        return cmd

    @classmethod
    def _exec_stream_in(cls, cmd) -> Optional[io.BufferedWriter]:
        process = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        errors = []
        while True:
            output = cls._exec_input_bin(process, errors)
            if output is False:
                break
            elif output is not None:
                return output
        return None

    @staticmethod
    def _exec_input_bin(process, errors):
        stderr = io.TextIOWrapper(process.stderr).readline()
        rc = process.poll()
        if stderr:
            error = stderr.strip()
            errors.append(error[0].upper() + error[1:])
        if rc is not None and rc != 0:
            raise Exception('\n'.join(errors))
        elif stderr != '':
            return None
        else:
            return process.stdin


class LoadKey(Command):

    def __call__(self, *args, **kwargs):
        """Load the dataset key

        Keyword arguments:
            ds: Filesystem -- ZFS filesystem to unload the key for, if None,
                unload keys for all filesystems (default None)
            location: str -- key location (default None)
            recursive: bool -- whether to unload keys for all child datasets (default False)

        Returns:
            None
        """
        return self._load_key(*args, **kwargs)

    @classmethod
    def _load_key(cls, ds: Optional[Filesystem], location: Optional[str] = None, recursive: bool = False) -> None:
        if ds is not None:
            cmd = [ZFS_BIN, 'load-key'] + cls._get_options(location=location, recursive=recursive) + [str(ds)]
        else:
            if location is not None:
                raise ValueError('Key location cannot be explicitly specified when loading all keys')
            if recursive:
                raise ValueError('Recursive cannot be specified when loading all keys')

            cmd = [ZFS_BIN, 'load-key', '-a']

        return cls._exec(cmd)

    @classmethod
    def _get_options(cls, **kwargs):
        cmd = []
        location = kwargs.get('location', None)
        recursive = kwargs.get('recursive', False)
        if location and recursive:
            raise ValueError('Key location cannot be explicitly specified when loading keys recursively')
        elif location:
            cmd += ['-L ' + location]
        elif recursive:
            cmd += ['-r']

        return cmd


class UnLoadKey(Command):

    def __call__(self, *args, **kwargs):
        """Unload the dataset key

        Keyword arguments:
            ds: Filesystem -- ZFS filesystem to unload the key for, if None,
                unload keys for all filesystems (default None)
            recursive: bool -- whether to unload keys for all child datasets (default False)

        Returns:
            None
        """
        return self._unload_key(*args, **kwargs)

    @classmethod
    def _unload_key(cls, ds: Optional[Filesystem], recursive: bool = False) -> None:
        if ds is not None:
            cmd = [ZFS_BIN, 'unload-key'] + cls._get_options(recursive=recursive) + [str(ds)]
        else:
            if recursive:
                raise ValueError('Recursive cannot be specified when unloading all keys')

            cmd = [ZFS_BIN, 'unload-key', '-a']

        return cls._exec(cmd)

    @classmethod
    def _get_options(cls, **kwargs):
        return ['-r'] if kwargs.get('recursive', False) else []


class ChangeKey(Command):

    def __call__(self, *args, **kwargs):
        """Change the dataset key

        Keyword arguments:
            ds: Filesystem -- ZFS filesystem to change the key for
            inherit: bool -- inhherit the key from parent dataset (default False)
            load: bool -- load the key before changing it (default False)
            location: str -- key location (default None)
            format: str -- key format (default None)
            iterations: int -- the number of PBKDF2 iterations (default None)

        Returns:
            None
        """
        return self._change_key(*args, **kwargs)

    @classmethod
    def _change_key(cls, ds: Filesystem, inherit: bool = False, load: bool = False, location: Optional[str] = None,
                    fmt: Optional[str] = None, iterations: Optional[int] = None) -> None:
        cmd = [ZFS_BIN, 'change-key'] + \
              cls._get_options(inherit=inherit, load=load, location=location,
                               fmt=fmt, iterations=iterations) + [str(ds)]

        return cls._exec(cmd)

    @classmethod
    def _get_options(cls, **kwargs):
        cmd = []
        location = kwargs.get('location', None)
        fmt = kwargs.get('fmt', None)
        iterations = kwargs.get('iterations', None)
        if kwargs.get('inherit', None):
            if location:
                raise ValueError('Key location cannot be specified when inheriting keys')
            if fmt:
                raise ValueError('Key format cannot be specified when inheriting keys')
            if iterations:
                raise ValueError('Number of pbkdf2 iterations cannot be specified when inheriting keys')
            cmd += ['-i']
        if kwargs.get('load', None):
            cmd += ['-l']
        if location:
            cmd += ['-o keylocation=' + location]
        if fmt:
            cmd += ['-o keyformat=' + fmt]
        if iterations:
            cmd += ['-o pbkdf2iters=' + str(iterations)]

        return cmd


class Mount(Command, PropertyCommand):

    def __call__(self, *args, **kwargs):
        """Mount ZFS dataset

        Keyword arguments:
            ds: Filesystem -- ZFS filesystem to mount, if None, mount all ZFS filesystems (default None)
            flags: str | [str] -- mount flags (default None)
            properties: str | [str] -- mount properties (default None)
            overlay: bool -- proceed even if the mount point is not empty (default False)
            load_keys: bool -- whether to load keys for each mounted filesystem (default False)
            force: bool -- attempt to mount the filesystems that normally would not be mounted (default False)

        Returns:
            None
        """
        return self._mount(*args, **kwargs)

    @classmethod
    def _mount(cls, ds: Optional[Filesystem], flags: Optional[StringList], properties: Optional[Properties],
               overlay: bool = False, load_keys: bool = False, force: bool = False) -> None:
        if ds is not None:
            cmd = [ZFS_BIN, 'mount'] + cls._get_options(overlay=overlay, load_keys=load_keys, force=force) + \
                  cls._get_properties(flags, properties) + [str(ds)]
        else:
            cmd = [ZFS_BIN, 'mount', '-a']

        return cls._exec(cmd)

    @classmethod
    def _get_options(cls, **kwargs):
        cmd = []
        if kwargs.get('overlay', None):
            cmd += ['-O']
        if kwargs.get('load_keys', None):
            cmd += ['-l']
        if kwargs.get('force', None):
            cmd += ['-f']

        return cmd

    @classmethod
    def _get_properties(cls, flags: Optional[StringList], properties: Optional[Properties]):
        return cls._get_flags(flags) + cls._get_props(properties, True)

    @staticmethod
    def _get_flags(flags: Optional[StringList]):
        return [f if y else '-o' for f in flags for y in range(2)]


class UnMount(Command):

    def __call__(self, *args, **kwargs):
        """Unmount ZFS filesystem

        Keyword arguments:
            ds: Filesystem -- ZFS filesystem to unmount, if None, unmount all ZFS filesystems (default None)
            force: bool -- forcefully unmount the file system, even if it is currently in use (default False)
            unload_keys: bool -- unload keys for any encryption roots being unmounted (default False)

        Returns:
            None
        """
        return self._unmount(*args, **kwargs)

    @classmethod
    def _unmount(cls, ds: Optional[Filesystem], force: bool = False, unload_keys: bool = False) -> None:
        if ds is not None:
            cmd = [ZFS_BIN, 'unmount'] + cls._get_options(force=force, unload_keys=unload_keys) + [str(ds)]
        else:
            cmd = [ZFS_BIN, 'unmount', '-a']

        return cls._exec(cmd)

    @classmethod
    def _get_options(cls, **kwargs):
        cmd = []
        if kwargs.get('force', None):
            cmd += ['-f']
        if kwargs.get('unload_keys', None):
            cmd += ['-u']

        return cmd
