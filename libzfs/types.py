import re
from typing import Optional, Dict, Iterable, Tuple, Union


class PropertyNames:
    apple = ['com.apple.browse', 'com.apple.ignoreowner', 'com.apple.mimic', 'com.apple.devdisk']

    dataset = ['available', 'checksum', 'compression', 'compressratio', 'context', 'copies', 'createtxg', 'creation',
               'dedup', 'defcontext', 'encryption', 'encryptionroot', 'fscontext', 'guid', 'keyformat', 'keylocation',
               'keystatus',
               'logbias', 'logicalreferenced', 'logicalused', 'mlslabel', 'objsetid', 'pbkdf2iters', 'primarycache',
               'readonly', 'redundant_metadata', 'refcompressratio', 'referenced', 'refreservation', 'reservation',
               'rootcontext', 'secondarycache', 'snapdev', 'snapshot_count', 'snapshot_limit', 'sync', 'type', 'used',
               'usedbychildren', 'usedbydataset', 'usedbyrefreservation', 'usedbysnapshots', 'volmode', 'written']

    filesystem = ['aclmode', 'aclinherit', 'acltype', 'atime', 'canmount', 'casesensitivity', 'devices', 'dnodesize',
                  'exec', 'filesystem_count', 'filesystem_limit', 'mounted', 'mountpoint', 'nbmand', 'normalization',
                  'overlay', 'quota', 'recordsize', 'refquota', 'relatime', 'setuid', 'sharenfs', 'sharesmb',
                  'snapdir', 'special_small_blocks', 'utf8only', 'version', 'vscan', 'xattr', 'zoned']

    volume = ['volblocksize', 'volsize']

    snapshot = ['clones', 'compressratio', 'context', 'createtxg', 'creation', 'defcontext', 'defer_destroy',
                'encryption', 'fscontext', 'guid', 'logicalreferenced', 'mlslabel', 'objsetid', 'primarycache',
                'refcompressratio', 'referenced', 'rootcontext', 'secondarycache', 'type', 'used', 'userrefs',
                'written']

    fs_snap = ['acltype', 'casesensitivity', 'devices', 'exec', 'nbmand', 'normalization', 'setuid', 'utf8only',
               'version', 'xattr']

    vol_snap = ['volsize']

    bookmark = ['createtxg', 'creation', 'guid', 'logicalreferenced', 'referenced', 'type']

    mutable = ['aclinherit', 'acltype', 'atime', 'canmount', 'casesensitivity', 'checksum', 'compression', 'copies',
               'devices', 'exec', 'filesystem_limit', 'mountpoint', 'nbmand', 'normalization', 'primarycache',
               'quota', 'readonly', 'recordsize', 'refquota', 'refreservation', 'reservation', 'secondarycache',
               'setuid', 'sharenfs', 'sharesmb', 'snapdir', 'snapshot_limit', 'utf8only', 'version', 'volblocksize',
               'volsize', 'vscan', 'xattr', 'zoned']

    all = apple + dataset + filesystem + volume + snapshot + fs_snap + vol_snap + bookmark + ['all']


class Validate:
    pool = r'([a-zA-Z][\w\._\-]*)'
    ds = r'(/[\w][\w\.:_\-]*)'
    name = r'[\w\.:_\-]+'
    snaps = r'[\w\.:_\-]*([\w\.:_\-]|%)*[\w\.:_\-]*(,[\w\.:_\-]*([\w\.:_\-]|%)*[\w\.:_\-]*)*'

    @staticmethod
    def zfsname(s: str) -> None:
        if not bool(re.match('^' + Validate.name + '$', s)):
            raise ValueError(s + ' is not a valid ZFS name')

    @staticmethod
    def type(s: str) -> None:
        if s.lower() not in ['filesystem', 'snapshot', 'volume', 'bookmark', 'all']:
            raise ValueError(s + ' is not a valid ZFS type')

    @staticmethod
    def source(s: str) -> None:
        if s.lower() not in ['local', 'default', 'inherited', 'temporary', 'received', 'all']:
            raise ValueError(s + ' is not a valid ZFS type')

    @staticmethod
    def propfield(s: str) -> None:
        if s.lower() not in ['name', 'property', 'value', 'received', 'source', 'all']:
            raise ValueError(s + ' is not a valid field')

    @staticmethod
    def attribute(s: str) -> None:
        try:
            Validate.zfsname(s)
        except ValueError:
            raise ValueError(s + ' is not a valid ZFS property name')
        if (':' not in s) and (s not in PropertyNames.all):
            raise ValueError(s + ' is not a valid ZFS property name')

    @staticmethod
    def property(s: str) -> None:
        Validate.attribute(s)

    @staticmethod
    def dataset(s: str) -> None:
        if not bool(re.match('^' + Validate.pool + Validate.ds + '*$', s)):
            raise ValueError(s + ' is not a valid ZFS dataset name')

    @staticmethod
    def snapshot(s: str) -> None:
        if not bool(re.match('^' + Validate.pool + Validate.ds + '*@' + Validate.name + '$', s)):
            raise ValueError(s + ' is not a valid ZFS snapshot name')

    @staticmethod
    def snapshots(s: str) -> None:
        if not bool(re.match('^' + Validate.pool + Validate.ds + '*@' + Validate.snaps + '$', s)):
            raise ValueError(s + ' is not a valid ZFS snapshot name')

    @staticmethod
    def bookmark(s: str) -> None:
        if not bool(re.match('^' + Validate.pool + Validate.ds + '*#' + Validate.name + '$', s)):
            raise ValueError(s + ' is not a valid ZFS bookmark name')


class Property:

    def __init__(self, value: str, source: Optional[str] = None, received: Optional[str] = None):
        self._value = value
        self._source = source
        self._received = received

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    def source(self):
        return self._source

    @property
    def received(self):
        return self._received

    def __str__(self):
        return self._value

    def __repr__(self):
        return self._value


PropertyValue = Union[Property, str]
Properties = Dict[str, PropertyValue]
PropertyItem = Tuple[str, Property]


class ZFS:

    _prop_names = []

    def __init__(self, name: str, properties: Optional[Properties] = None) -> None:
        self._name = name
        self._props = {}
        self._user_props = {}
        if properties is not None:
            self.update(properties)

    @property
    def name(self) -> str:
        return self._name

    @property
    def properties(self) -> Iterable[PropertyItem]:
        yield from ((self._prop_names[k], v) for (k, v) in self._props.items())
        yield from self._user_props

    def reset(self):
        self._props = {}
        self._user_props = {}

    def update(self, properties: Properties):
        for k, v in properties.items():
            Validate.attribute(k)
            if v is None:
                continue
            if not isinstance(v, Property):
                v = Property(v)
            try:
                idx = self._prop_names.index(k)
                self._props[idx] = v
            except ValueError:
                if ':' in k:
                    self._user_props[k] = v
                else:
                    raise ValueError(k + ' is not a valid ' + self.__class__.__name__ + ' property')

    def __getitem__(self, k: str) -> Optional[Property]:
        Validate.attribute(k)
        try:
            i = self._prop_names.index(k)
            return self._props.get(i, None)
        except ValueError:
            if ':' in k:
                return self._user_props.get(k, None)
            else:
                raise ValueError(k + ' is not a valid ' + self.__class__.__name__ + ' property')

    def __getattr__(self, k: str) -> Optional[Property]:
        Validate.attribute(k)
        try:
            i = self._prop_names.index(k)
            return self._props.get(i, None)
        except ValueError:
            raise ValueError(k + ' is not a valid ' + self.__class__.__name__ + ' property')

    def __str__(self) -> str:
        return self._name

    def __repr__(self) -> str:
        return self._name + ' (' + self._type + ')'

    @staticmethod
    def from_name(name: str, dstype: Optional[str] = None, properties: Optional[Properties] = None):
        d = {
            'filesystem': Filesystem,
            'volume': Volume,
            'dataset': Dataset,
            'snapshot': Snapshot,
            'bookmark': Bookmark
        }
        if dstype is not None:
            try:
                return d[dstype].from_name(name, properties=properties)
            except KeyError:
                raise ValueError(dstype + ' is not a valid ZFS type')
        else:
            for dstype in [Dataset, Snapshot, Bookmark]:
                try:
                    return dstype.from_name(name, properties=properties)
                except ValueError:
                    pass
        raise ValueError('Could not guess ZFS object type')


class Dataset(ZFS):
    _prop_names = PropertyNames.dataset

    def __init__(self, name: str, properties: Optional[Properties] = None) -> None:
        Validate.dataset(name)
        super().__init__(name, properties)

    @classmethod
    def from_name(cls, name: str, dstype=None, properties: Optional[Properties] = None):
        return cls(name, properties)


class Filesystem(Dataset):
    _prop_names = PropertyNames.dataset + PropertyNames.filesystem + PropertyNames.apple

    def __init__(self, name: str, properties: Optional[Properties] = None) -> None:
        super().__init__(name, properties)
        self._type = 'filesystem'


class Volume(Dataset):
    _prop_names = PropertyNames.dataset + PropertyNames.volume

    def __init__(self, name: str, properties: Optional[Properties] = None) -> None:
        super().__init__(name, properties)
        self._type = 'volume'


class Snapshot(ZFS):
    __prop_names = PropertyNames.snapshot

    def __init__(self, ds: Dataset, name: str, properties: Optional[Properties] = None) -> None:
        if not isinstance(ds, Dataset):
            raise ValueError('Expected Filesystem, Volume or Dataset, got ' + type(ds).__name__ + ' instead')
        name = ds.name + '@' + name.strip('@')
        Validate.snapshot(name)
        self._dataset = ds
        self._type = 'snapshot'
        super().__init__(name, properties)

    @property
    def dataset(self) -> Dataset:
        return self._dataset

    @property
    def short(self) -> str:
        return self._name.split('@')[1]

    @classmethod
    def from_name(cls, name: str, dstype='snapshot', properties: Optional[Properties] = None):
        ds, name = name.split('@')
        return cls(Dataset(ds), name, properties)

    @property
    def _prop_names(self):
        if isinstance(self.dataset, Filesystem):
            return self.__prop_names + PropertyNames.fs_snap
        elif isinstance(self.dataset, Volume):
            return self.__prop_names + PropertyNames.vol_snap
        else:
            return self.__prop_names + PropertyNames.fs_snap + PropertyNames.vol_snap


class SnapshotRange:

    def __init__(self, dataset: Optional[Dataset] = None, first: Optional[Snapshot] = None,
                 last: Optional[Snapshot] = None):

        if dataset is not None and not isinstance(dataset, Dataset):
            raise ValueError('Expected Filesystem, Volume or Dataset, got ' + type(dataset).__name__ + ' instead')
        for snap in first, last:
            if snap is not None and not isinstance(dataset, Snapshot):
                raise ValueError('Expected Snapshot, got ' + type(dataset).__name__ + ' instead')

        self._dataset = dataset
        self._first = first
        self._last = last
        for snap in [self._first, self._last]:
            if snap is not None:
                if self._dataset is None:
                    self._dataset = snap.dataset
                elif str(snap.dataset) != str(self._dataset):
                    raise ValueError('Snapshots must come from the same dataset')
        if self._dataset is None:
            raise ValueError('Could not determine snapshot dataset')

    @property
    def dataset(self):
        return self._dataset

    @property
    def first(self):
        return self._first

    @property
    def last(self):
        return self._last

    @property
    def name(self):
        return self.dataset + '@' + self.short

    @property
    def short(self):
        return self._first.short if self._first else '' + '%' + self._last.short if self._last else ''

    @classmethod
    def from_name(cls, name: Optional[str] = None, first: Optional[str] = None, last: Optional[str] = None):
        parts = name.split('@')
        if len(parts) == 2:
            if first or last:
                raise ValueError('Range can be specified either by name or first and last snapshot')
            name = parts[0]
            snaps = parts[1].split('%')
            if len(snaps) == 2:
                first, last = snaps
            else:
                raise ValueError(name + ' is not a valid ZFS snapshot range specification')

        ds = Dataset.from_name(name) if name else None
        if first:
            first = Snapshot.from_name(first) if '@' in first else Snapshot(ds, first)
        else:
            first = None
        if last:
            last = Snapshot.from_name(last) if '@' in last else Snapshot(ds, last)
        else:
            last = None

        return cls(ds, first, last)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


class Bookmark(ZFS):
    _prop_names = PropertyNames.bookmark

    def __init__(self, ds: Dataset, name: str, properties: Optional[Properties] = None):
        if not isinstance(ds, Dataset):
            raise ValueError('Expected Filesystem, Volume or Dataset, got ' + type(ds).__name__ + ' instead')

        name = ds.name + '#' + name.strip('#')
        Validate.bookmark(name)
        super().__init__(name, properties)
        self._dataset = ds
        self._type = 'bookmark'

    @property
    def dataset(self) -> Dataset:
        return self._dataset

    @property
    def short(self) -> str:
        return self._name.split('#')[1]

    @classmethod
    def from_name(cls, name: str, dstype='bookmark', properties: Optional[Properties] = None):
        ds, name = name.split('#')
        return cls(Dataset(ds), name, properties)
