"""Unit tests for libzfseasy.types module."""

import copy
import pickle
import pytest
from libzfseasy.types import (
    Validate, Property, PropertyNames, ZFS, Dataset, Filesystem, 
    Volume, Snapshot, SnapshotRange, Bookmark, InvalidNameError
)


class TestValidate:
    """Tests for Validate class."""
    
    @pytest.mark.unit
    def test_zfsname_valid(self):
        """Test validation of valid ZFS names."""
        valid_names = ['pool', 'pool1', 'my_pool', 'my-pool', 'my.pool', 'pool:test']
        for name in valid_names:
            Validate.zfsname(name)  # Should not raise
    
    @pytest.mark.unit
    def test_zfsname_invalid(self):
        """Test validation of invalid ZFS names."""
        invalid_names = ['', 'pool with spaces', 'pool@snap', 'pool#bookmark', 'pool/dataset']
        for name in invalid_names:
            with pytest.raises(ValueError):
                Validate.zfsname(name)
    
    @pytest.mark.unit
    def test_type_valid(self):
        """Test validation of valid ZFS types."""
        valid_types = ['filesystem', 'snapshot', 'volume', 'bookmark', 'all', 'FILESYSTEM']
        for dstype in valid_types:
            Validate.type(dstype)  # Should not raise
    
    @pytest.mark.unit
    def test_type_invalid(self):
        """Test validation of invalid ZFS types."""
        with pytest.raises(ValueError):
            Validate.type('invalid')
    
    @pytest.mark.unit
    def test_source_valid(self):
        """Test validation of valid ZFS property sources."""
        valid_sources = ['local', 'default', 'inherited', 'temporary', 'received', 'all']
        for source in valid_sources:
            Validate.source(source)  # Should not raise
    
    @pytest.mark.unit
    def test_source_invalid(self):
        """Test validation of invalid ZFS property sources."""
        with pytest.raises(ValueError):
            Validate.source('invalid')
    
    @pytest.mark.unit
    def test_dataset_valid(self):
        """Test validation of valid dataset names."""
        valid_datasets = ['pool', 'pool/dataset', 'pool/parent/child', 'pool/my_dataset', 'pool/my-dataset']
        for ds in valid_datasets:
            Validate.dataset(ds)  # Should not raise
    
    @pytest.mark.unit
    def test_dataset_invalid(self):
        """Test validation of invalid dataset names."""
        invalid_datasets = ['', 'pool@snap', 'pool#bookmark', '/dataset', 'pool//dataset']
        for ds in invalid_datasets:
            with pytest.raises(ValueError):
                Validate.dataset(ds)
    
    @pytest.mark.unit
    def test_snapshot_valid(self):
        """Test validation of valid snapshot names."""
        valid_snapshots = ['pool@snap', 'pool/dataset@snap', 'pool/dataset@my_snap-1']
        for snap in valid_snapshots:
            Validate.snapshot(snap)  # Should not raise
    
    @pytest.mark.unit
    def test_snapshot_invalid(self):
        """Test validation of invalid snapshot names."""
        invalid_snapshots = ['pool', 'pool/dataset', '@snap', 'pool@', 'pool@@snap']
        for snap in invalid_snapshots:
            with pytest.raises(ValueError):
                Validate.snapshot(snap)
    
    @pytest.mark.unit
    def test_bookmark_valid(self):
        """Test validation of valid bookmark names."""
        valid_bookmarks = ['pool#bookmark', 'pool/dataset#bookmark', 'pool/dataset#my_bookmark-1']
        for bm in valid_bookmarks:
            Validate.bookmark(bm)  # Should not raise
    
    @pytest.mark.unit
    def test_bookmark_invalid(self):
        """Test validation of invalid bookmark names."""
        invalid_bookmarks = ['pool', 'pool/dataset', '#bookmark', 'pool#', 'pool##bookmark']
        for bm in invalid_bookmarks:
            with pytest.raises(ValueError):
                Validate.bookmark(bm)
    
    @pytest.mark.unit
    def test_attribute_valid(self):
        """Test validation of valid attributes."""
        valid_attrs = ['compression', 'mountpoint', 'quota', 'custom:property']
        for attr in valid_attrs:
            Validate.attribute(attr)  # Should not raise
    
    @pytest.mark.unit
    def test_attribute_invalid(self):
        """Test validation of invalid attributes."""
        with pytest.raises(ValueError):
            Validate.attribute('invalid_property')

    @pytest.mark.unit
    def test_propfield_valid(self):
        valid = ['name', 'property', 'value', 'received', 'source', 'all']
        for f in valid:
            Validate.propfield(f)

    @pytest.mark.unit
    def test_propfield_invalid(self):
        with pytest.raises(ValueError):
            Validate.propfield('bogus')

    @pytest.mark.unit
    def test_attribute_invalid_name_with_space(self):
        with pytest.raises(ValueError, match='is not a valid ZFS property name'):
            Validate.attribute('invalid name')

    @pytest.mark.unit
    def test_property_validator_valid(self):
        Validate.property('compression')

    @pytest.mark.unit
    def test_property_validator_invalid(self):
        with pytest.raises(ValueError):
            Validate.property('invalid_property')

    @pytest.mark.unit
    def test_snapshots_valid(self):
        Validate.snapshots('pool/ds@snap1')
        Validate.snapshots('pool/ds@snap1,snap2')
        Validate.snapshots('pool/ds@%snap2')
        Validate.snapshots('pool/ds@snap1%')

    @pytest.mark.unit
    def test_snapshots_invalid(self):
        with pytest.raises(ValueError):
            Validate.snapshots('pool/ds')
        with pytest.raises(ValueError):
            Validate.snapshots('@snap')


class TestProperty:
    """Tests for Property class."""
    
    @pytest.mark.unit
    def test_property_creation(self):
        """Test creating a Property object."""
        prop = Property('lz4', source='local', received='inherited')
        assert prop.value == 'lz4'
        assert prop.source == 'local'
        assert prop.received == 'inherited'
    
    @pytest.mark.unit
    def test_property_str_repr(self):
        """Test string representation of Property."""
        prop = Property('lz4')
        assert str(prop) == 'lz4'
        assert repr(prop) == 'lz4'
    
    @pytest.mark.unit
    @pytest.mark.parametrize('value, text', [(5, '5'), (0, '0'), (1.5, '1.5'), (True, 'True'), ('lz4', 'lz4')])
    def test_property_str_repr_of_non_string_value(self, value, text):
        """__str__/__repr__ always return a str (a non-str return raises TypeError); `.value` stays as given."""
        prop = Property(value)
        assert str(prop) == text
        assert repr(prop) == text
        assert isinstance(str(prop), str) and isinstance(repr(prop), str)
        assert prop.value == value
        assert '%s' % prop == text
        assert ','.join(str(p) for p in [prop, prop]) == text + ',' + text

    @pytest.mark.unit
    def test_property_value_setter(self):
        """Test setting property value."""
        prop = Property('lz4')
        prop.value = 'gzip'
        assert prop.value == 'gzip'


class TestZFS:
    """Tests for ZFS base class methods."""

    @pytest.mark.unit
    def test_zfs_reset(self):
        fs = Filesystem('pool/fs', {'compression': 'lz4'})
        assert fs['compression'] is not None
        fs.reset()
        assert list(fs.properties) == []

    @pytest.mark.unit
    def test_update_user_prop_delete(self):
        fs = Filesystem('pool/fs', {'custom:tag': 'v1'})
        assert fs['custom:tag'].value == 'v1'
        fs.update({'custom:tag': None})
        assert fs['custom:tag'] is None

    @pytest.mark.unit
    def test_getitem_user_prop(self):
        fs = Filesystem('pool/fs', {'custom:tag': 'v1'})
        assert fs['custom:tag'].value == 'v1'

    @pytest.mark.unit
    def test_getitem_wrong_type_prop_raises(self):
        fs = Filesystem('pool/fs')
        with pytest.raises(ValueError):
            _ = fs['volsize']

    @pytest.mark.unit
    def test_getattr_wrong_type_prop_raises(self):
        """__getattr__ raises AttributeError (not ValueError) for a property
        that is a valid ZFS property in general but not applicable to this
        object's type, matching Python's attribute-access protocol so that
        hasattr()/getattr(default=)/copy.deepcopy() work correctly. See also
        TestGetAttrAttributeErrorRegression below."""
        fs = Filesystem('pool/fs')
        with pytest.raises(AttributeError):
            _ = fs.volsize

    @pytest.mark.unit
    def test_contains_known_prop_present(self):
        fs = Filesystem('pool/fs', {'compression': 'lz4'})
        assert 'compression' in fs

    @pytest.mark.unit
    def test_contains_known_prop_absent(self):
        fs = Filesystem('pool/fs')
        assert 'compression' not in fs

    @pytest.mark.unit
    def test_contains_user_prop(self):
        fs = Filesystem('pool/fs', {'custom:tag': 'v'})
        assert 'custom:tag' in fs

    @pytest.mark.unit
    def test_contains_wrong_type_prop_raises(self):
        fs = Filesystem('pool/fs')
        with pytest.raises(ValueError):
            _ = 'volsize' in fs

    @pytest.mark.unit
    def test_repr(self):
        fs = Filesystem('pool/fs')
        assert repr(fs) == 'pool/fs (filesystem)'


class TestDataset:
    """Tests for Dataset class."""
    
    @pytest.mark.unit
    def test_dataset_creation(self, sample_pool):
        """Test creating a Dataset object."""
        ds = Dataset(f'{sample_pool}/dataset')
        assert ds.name == f'{sample_pool}/dataset'
        assert str(ds) == f'{sample_pool}/dataset'
    
    @pytest.mark.unit
    def test_dataset_invalid_name(self):
        """Test creating Dataset with invalid name."""
        with pytest.raises(ValueError):
            Dataset('invalid@name')
    
    @pytest.mark.unit
    def test_dataset_with_properties(self, sample_pool, dataset_properties):
        """Test creating Dataset with properties."""
        ds = Dataset(f'{sample_pool}/dataset', dataset_properties)
        assert ds['compression'].value == 'lz4'
        assert ds['reservation'].value == '1G'
    
    @pytest.mark.unit
    def test_dataset_property_access(self, sample_pool):
        """Test accessing dataset properties."""
        ds = Dataset(f'{sample_pool}/dataset', {'compression': 'lz4'})
        assert ds['compression'].value == 'lz4'
        assert ds.compression.value == 'lz4'
    
    @pytest.mark.unit
    def test_dataset_update_properties(self, sample_pool):
        """Test updating dataset properties."""
        ds = Dataset(f'{sample_pool}/dataset')
        ds.update({'compression': 'gzip', 'reservation': '2G'})
        assert ds['compression'].value == 'gzip'
        assert ds['reservation'].value == '2G'

    @pytest.mark.unit
    def test_dataset_rejects_filesystem_volume_props(self, sample_pool):
        """Ensure Dataset does not accept filesystem/volume-only properties."""
        ds = Dataset(f'{sample_pool}/dataset')
        with pytest.raises(ValueError):
            ds.update({'mountpoint': '/mnt/test'})
        with pytest.raises(ValueError):
            ds.update({'quota': '10G'})
    
    @pytest.mark.unit
    def test_dataset_from_name(self):
        """Test creating Dataset from name."""
        ds = Dataset.from_name('pool/dataset')
        assert ds.name == 'pool/dataset'


class TestFilesystem:
    """Tests for Filesystem class."""
    
    @pytest.mark.unit
    def test_filesystem_creation(self, sample_pool):
        """Test creating a Filesystem object."""
        fs = Filesystem(f'{sample_pool}/filesystem')
        assert fs.name == f'{sample_pool}/filesystem'
        assert fs._type == 'filesystem'
    
    @pytest.mark.unit
    def test_filesystem_with_properties(self, sample_pool, sample_properties):
        """Test creating Filesystem with properties."""
        fs = Filesystem(f'{sample_pool}/filesystem', sample_properties)
        assert fs['compression'].value == 'lz4'
        assert fs['mountpoint'].value == '/mnt/test'
        assert fs['quota'].value == '10G'


class TestVolume:
    """Tests for Volume class."""
    
    @pytest.mark.unit
    def test_volume_creation(self, sample_pool):
        """Test creating a Volume object."""
        vol = Volume(f'{sample_pool}/volume')
        assert vol.name == f'{sample_pool}/volume'
        assert vol._type == 'volume'
    
    @pytest.mark.unit
    def test_volume_with_properties(self, sample_pool):
        """Test creating Volume with properties."""
        vol = Volume(f'{sample_pool}/volume', {'volsize': '10G'})
        assert vol['volsize'].value == '10G'


class TestSnapshot:
    """Tests for Snapshot class."""
    
    @pytest.mark.unit
    def test_snapshot_creation(self, sample_filesystem):
        """Test creating a Snapshot object."""
        snap = Snapshot(sample_filesystem, 'snap1')
        assert snap.name == f'{sample_filesystem.name}@snap1'
        assert snap.short == 'snap1'
        assert snap.dataset == sample_filesystem
        assert snap._type == 'snapshot'
    
    @pytest.mark.unit
    def test_snapshot_creation_with_at_sign(self, sample_filesystem):
        """Test creating Snapshot with @ prefix."""
        snap = Snapshot(sample_filesystem, '@snap1')
        assert snap.name == f'{sample_filesystem.name}@snap1'
        assert snap.short == 'snap1'
    
    @pytest.mark.unit
    def test_snapshot_from_name(self):
        """Test creating Snapshot from full name."""
        snap = Snapshot.from_name('pool/dataset@snap1')
        assert snap.name == 'pool/dataset@snap1'
        assert snap.short == 'snap1'
        assert snap.dataset.name == 'pool/dataset'
    
    @pytest.mark.unit
    def test_snapshot_invalid_dataset(self):
        """Test creating Snapshot with invalid dataset."""
        with pytest.raises(ValueError):
            Snapshot('not_a_dataset', 'snap1')

    @pytest.mark.unit
    def test_snapshot_prop_names_filesystem(self):
        fs = Filesystem('pool/fs')
        snap = Snapshot(fs, 'snap1')
        assert 'acltype' in snap._prop_names
        assert 'volsize' not in snap._prop_names

    @pytest.mark.unit
    def test_snapshot_prop_names_volume(self):
        vol = Volume('pool/vol')
        snap = Snapshot(vol, 'snap1')
        assert 'volsize' in snap._prop_names
        assert 'acltype' not in snap._prop_names

    @pytest.mark.unit
    def test_snapshot_prop_names_base_dataset(self):
        ds = Dataset('pool/ds')
        snap = Snapshot(ds, 'snap1')
        assert 'acltype' in snap._prop_names
        assert 'volsize' in snap._prop_names


class TestSnapshotRange:
    """Tests for SnapshotRange class."""
    
    @pytest.mark.unit
    def test_snapshot_range_creation(self, sample_filesystem):
        """Test creating a SnapshotRange object."""
        snap1 = Snapshot(sample_filesystem, 'snap1')
        snap2 = Snapshot(sample_filesystem, 'snap2')
        range_obj = SnapshotRange(first=snap1, last=snap2)
        assert range_obj.dataset == sample_filesystem
        assert range_obj.first == snap1
        assert range_obj.last == snap2
        assert range_obj.short == 'snap1%snap2'
    
    @pytest.mark.unit
    def test_snapshot_range_from_name(self):
        """Test creating SnapshotRange from name."""
        range_obj = SnapshotRange.from_name('pool/dataset@snap1%snap2')
        assert range_obj.dataset.name == 'pool/dataset'
        assert range_obj.first.short == 'snap1'
        assert range_obj.last.short == 'snap2'
    
    @pytest.mark.unit
    def test_snapshot_range_open_ended(self, sample_filesystem):
        """Test creating open-ended SnapshotRange."""
        snap1 = Snapshot(sample_filesystem, 'snap1')
        range_obj = SnapshotRange(first=snap1, last=None)
        assert range_obj.short == 'snap1%'
        
        range_obj = SnapshotRange(first=None, last=snap1)
        assert range_obj.short == '%snap1'
    
    @pytest.mark.unit
    def test_snapshot_range_different_datasets(self, sample_pool):
        """Test creating SnapshotRange with snapshots from different datasets."""
        fs1 = Filesystem(f'{sample_pool}/fs1')
        fs2 = Filesystem(f'{sample_pool}/fs2')
        snap1 = Snapshot(fs1, 'snap1')
        snap2 = Snapshot(fs2, 'snap2')

        with pytest.raises(ValueError):
            SnapshotRange(first=snap1, last=snap2)

    @pytest.mark.unit
    def test_snapshot_range_invalid_dataset_type(self):
        with pytest.raises(ValueError, match='Expected Filesystem'):
            SnapshotRange(dataset='not_a_dataset')

    @pytest.mark.unit
    def test_snapshot_range_invalid_snap_type(self, sample_filesystem):
        with pytest.raises(ValueError, match='Expected Snapshot'):
            SnapshotRange(dataset=sample_filesystem, first='not_a_snap')

    @pytest.mark.unit
    def test_snapshot_range_no_dataset_raises(self):
        with pytest.raises(ValueError, match='Could not determine snapshot dataset'):
            SnapshotRange(first=None, last=None, dataset=None)

    @pytest.mark.unit
    def test_snapshot_range_name_property(self):
        r = SnapshotRange.from_name('pool/ds@snap1%snap2')
        assert str(r.name) == 'pool/ds@snap1%snap2'

    @pytest.mark.unit
    def test_snapshot_range_from_name_conflict_raises(self):
        with pytest.raises(ValueError, match='Range can be specified either'):
            SnapshotRange.from_name('pool/ds@snap1%snap2', first='pool/ds@snap1')

    @pytest.mark.unit
    def test_snapshot_range_from_name_no_percent_raises(self):
        with pytest.raises(ValueError, match='is not a valid ZFS snapshot range specification'):
            SnapshotRange.from_name('pool/ds@snap1')

    @pytest.mark.unit
    def test_snapshot_range_from_name_no_first(self):
        r = SnapshotRange.from_name('pool/ds@%snap2')
        assert r.first is None
        assert r.last.short == 'snap2'

    @pytest.mark.unit
    def test_snapshot_range_from_name_no_last(self):
        r = SnapshotRange.from_name('pool/ds@snap1%')
        assert r.first.short == 'snap1'
        assert r.last is None

    @pytest.mark.unit
    def test_snapshot_range_str_and_repr(self):
        r = SnapshotRange.from_name('pool/ds@snap1%snap2')
        assert str(r) == 'pool/ds@snap1%snap2'
        assert repr(r) == 'pool/ds@snap1%snap2'


class TestBookmark:
    """Tests for Bookmark class."""
    
    @pytest.mark.unit
    def test_bookmark_creation(self, sample_filesystem):
        """Test creating a Bookmark object."""
        bm = Bookmark(sample_filesystem, 'bookmark1')
        assert bm.name == f'{sample_filesystem.name}#bookmark1'
        assert bm.short == 'bookmark1'
        assert bm.dataset == sample_filesystem
        assert bm._type == 'bookmark'
    
    @pytest.mark.unit
    def test_bookmark_creation_with_hash(self, sample_filesystem):
        """Test creating Bookmark with # prefix."""
        bm = Bookmark(sample_filesystem, '#bookmark1')
        assert bm.name == f'{sample_filesystem.name}#bookmark1'
        assert bm.short == 'bookmark1'
    
    @pytest.mark.unit
    def test_bookmark_from_name(self):
        """Test creating Bookmark from full name."""
        bm = Bookmark.from_name('pool/dataset#bookmark1')
        assert bm.name == 'pool/dataset#bookmark1'
        assert bm.short == 'bookmark1'
        assert bm.dataset.name == 'pool/dataset'
    
    @pytest.mark.unit
    def test_bookmark_invalid_dataset(self):
        """Test creating Bookmark with invalid dataset."""
        with pytest.raises(ValueError):
            Bookmark('not_a_dataset', 'bookmark1')


class TestZFSFromName:
    """Tests for ZFS.from_name factory method."""
    
    @pytest.mark.unit
    def test_from_name_filesystem(self):
        """Test creating Filesystem from name."""
        obj = ZFS.from_name('pool/dataset', 'filesystem')
        assert isinstance(obj, Filesystem)
        assert obj.name == 'pool/dataset'
    
    @pytest.mark.unit
    def test_from_name_volume(self):
        """Test creating Volume from name."""
        obj = ZFS.from_name('pool/volume', 'volume')
        assert isinstance(obj, Volume)
        assert obj.name == 'pool/volume'
    
    @pytest.mark.unit
    def test_from_name_snapshot(self):
        """Test creating Snapshot from name."""
        obj = ZFS.from_name('pool/dataset@snap', 'snapshot')
        assert isinstance(obj, Snapshot)
        assert obj.name == 'pool/dataset@snap'
    
    @pytest.mark.unit
    def test_from_name_bookmark(self):
        """Test creating Bookmark from name."""
        obj = ZFS.from_name('pool/dataset#bookmark', 'bookmark')
        assert isinstance(obj, Bookmark)
        assert obj.name == 'pool/dataset#bookmark'
    
    @pytest.mark.unit
    def test_from_name_auto_detect(self):
        """Test auto-detecting type from name."""
        obj = ZFS.from_name('pool/dataset')
        assert isinstance(obj, Dataset)
        
        obj = ZFS.from_name('pool/dataset@snap')
        assert isinstance(obj, Snapshot)
        
        obj = ZFS.from_name('pool/dataset#bookmark')
        assert isinstance(obj, Bookmark)
    
    @pytest.mark.unit
    def test_from_name_invalid_type(self):
        """Test creating ZFS object with invalid type."""
        with pytest.raises(ValueError):
            ZFS.from_name('pool/dataset', 'invalid')


class TestGetAttrAttributeErrorRegression:
    """Regression tests: ZFS.__getattr__ must raise AttributeError (not ValueError)
    for a missing/invalid attribute, so that hasattr() and copy.deepcopy() behave
    correctly. Prior to the fix, __getattr__ let ValueError escape (via
    Validate.attribute), which hasattr() does not catch and which breaks
    copy.deepcopy()'s probing of dunder methods (e.g. __deepcopy__)."""

    @pytest.mark.unit
    def test_getattr_nonexistent_attr_raises_attributeerror(self):
        fs = Filesystem('pool/fs')
        with pytest.raises(AttributeError):
            _ = fs.nonexistent_attr

    @pytest.mark.unit
    def test_getattr_dunder_attr_raises_attributeerror(self):
        """copy/pickle probe dunder attributes like __deepcopy__ via getattr();
        these must raise AttributeError, not ValueError, or deepcopy() breaks."""
        fs = Filesystem('pool/fs')
        with pytest.raises(AttributeError):
            _ = fs.__deepcopy__

    @pytest.mark.unit
    def test_hasattr_returns_false_for_invalid_attr(self):
        fs = Filesystem('pool/fs')
        assert hasattr(fs, 'nonexistent_attr') is False

    @pytest.mark.unit
    def test_deepcopy_works(self):
        """copy.deepcopy() relies on getattr() raising AttributeError (not
        ValueError) when probing for __deepcopy__/__reduce_ex__/etc."""
        import copy
        fs = Filesystem('pool/fs', {'compression': 'lz4'})
        fs_copy = copy.deepcopy(fs)
        assert fs_copy is not fs
        assert fs_copy.name == fs.name
        assert fs_copy['compression'].value == 'lz4'


class TestValidateAcceptRejectTables:
    """Table-driven accept/reject coverage for the ASCII-only, fullmatch, space-aware validators.

    Every pattern is applied with ``re.fullmatch(..., re.ASCII)``: a trailing newline or a non-ASCII word character
    must be rejected. Spaces are allowed only *inside* dataset components and snapshot/bookmark short names.
    """

    @pytest.mark.unit
    @pytest.mark.parametrize('name', [
        'pool/ds', 'pool/my data', 'pool/a b/c d', 'pool/a  b',
    ])
    def test_dataset_accepts(self, name):
        Validate.dataset(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', [
        'pool/ds\n', 'pool/dś', '-pool', 'pool/-x', 'pool/.x', 'pool/..', 'pool/a/../b',
        'pool/ my', 'pool/my ', 'my pool/x',
    ])
    def test_dataset_rejects(self, name):
        with pytest.raises(ValueError):
            Validate.dataset(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', [
        'pool/ds@s', 'pool/ds@before upgrade', 'pool/ds@-x',
    ])
    def test_snapshot_accepts(self, name):
        Validate.snapshot(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', [
        'pool/ds@s\n', 'pool/ds@ lead', 'pool/ds@trail ', 'pool/ds@ś',
    ])
    def test_snapshot_rejects(self, name):
        with pytest.raises(ValueError):
            Validate.snapshot(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', [
        'ds@a%b', 'ds@a%b c', 'ds@a b%c d', 'ds@%b', 'ds@a%', 'ds@a,b%c',
    ])
    def test_snapshots_accepts(self, name):
        Validate.snapshots(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', [
        'ds@a %b', 'ds@a% b', 'ds@a%b\n',
    ])
    def test_snapshots_rejects(self, name):
        with pytest.raises(ValueError):
            Validate.snapshots(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', [
        'pool/ds#b', 'pool/ds#my mark',
    ])
    def test_bookmark_accepts(self, name):
        Validate.bookmark(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', [
        'pool/ds# x', 'pool/ds#x ', 'pool/ds#x\n',
    ])
    def test_bookmark_rejects(self, name):
        with pytest.raises(ValueError):
            Validate.bookmark(name)

    @pytest.mark.unit
    def test_attribute_and_zfsname_accept_plain_property(self):
        Validate.attribute('compression')
        Validate.zfsname('compression')

    @pytest.mark.unit
    @pytest.mark.parametrize('name', ['my prop:x', 'a b', 'com.apple.mimic\n'])
    def test_attribute_rejects(self, name):
        with pytest.raises(ValueError):
            Validate.attribute(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', ['my prop:x', 'a b', 'com.apple.mimic\n'])
    def test_zfsname_rejects(self, name):
        with pytest.raises(ValueError):
            Validate.zfsname(name)

    @pytest.mark.unit
    def test_pool_name_stays_space_free(self):
        with pytest.raises(ValueError):
            Validate.dataset('my pool')
        with pytest.raises(ValueError):
            Validate.snapshot('my pool@s')


class TestSpacedNamesConstruct:
    """Objects built from names containing inner spaces keep them in name/short."""

    @pytest.mark.unit
    def test_snapshot_from_name_with_space(self):
        snap = ZFS.from_name('tank/data@before upgrade', 'snapshot')
        assert isinstance(snap, Snapshot)
        assert snap.name == 'tank/data@before upgrade'
        assert snap.short == 'before upgrade'

    @pytest.mark.unit
    def test_bookmark_from_name_with_space(self):
        bm = ZFS.from_name('tank/data#my mark', 'bookmark')
        assert isinstance(bm, Bookmark)
        assert bm.name == 'tank/data#my mark'
        assert bm.short == 'my mark'

    @pytest.mark.unit
    def test_dataset_with_inner_spaces(self):
        fs = Filesystem('tank/my data/sub dir')
        assert fs.name == 'tank/my data/sub dir'

    @pytest.mark.unit
    def test_snapshot_range_from_name_with_spaces(self):
        rng = SnapshotRange.from_name('pool/ds@a b%c d')
        assert rng.first.short == 'a b'
        assert rng.last.short == 'c d'
        assert rng.short == 'a b%c d'
        assert rng.name == 'pool/ds@a b%c d'

    @pytest.mark.unit
    def test_snapshot_constructor_rejects_edge_spaces(self):
        ds = Filesystem('pool/ds')
        with pytest.raises(ValueError):
            Snapshot(ds, ' lead')
        with pytest.raises(ValueError):
            Snapshot(ds, 'trail ')

    @pytest.mark.unit
    def test_bookmark_constructor_rejects_edge_spaces(self):
        ds = Filesystem('pool/ds')
        with pytest.raises(ValueError):
            Bookmark(ds, ' lead')
        with pytest.raises(ValueError):
            Bookmark(ds, 'trail ')


class TestValidateDotsSnapshotsLength:
    """Review fixes: `.`/`..` short names, the `snapshots` list grammar, and the 255-character name cap."""

    @pytest.mark.unit
    @pytest.mark.parametrize('fn,sep', [(Validate.snapshot, '@'), (Validate.bookmark, '#')])
    @pytest.mark.parametrize('short', ['.', '..'])
    def test_dot_short_names_rejected(self, fn, sep, short):
        with pytest.raises(ValueError):
            fn('pool/ds' + sep + short)

    @pytest.mark.unit
    @pytest.mark.parametrize('fn,sep', [(Validate.snapshot, '@'), (Validate.bookmark, '#')])
    @pytest.mark.parametrize('short', ['a.b', '.x', '...', 'x.'])
    def test_dotted_short_names_accepted(self, fn, sep, short):
        fn('pool/ds' + sep + short)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', [
        'pool@', 'pool@%', 'pool@,,', 'pool@a,', 'pool@,a', 'pool@a%b%c', 'pool@..', 'pool@a%..', 'pool@..%b',
        'pool@.', 'pool@.%b', 'pool@a,..', 'pool@a%b,%',
    ])
    def test_snapshots_list_rejects(self, name):
        with pytest.raises(ValueError):
            Validate.snapshots(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', [
        'pool@a', 'pool@a%', 'pool@%b', 'pool@a%b', 'pool@a,b', 'pool@a,%b,c%d', 'pool@a.b,...',
    ])
    def test_snapshots_list_accepts(self, name):
        Validate.snapshots(name)

    @pytest.mark.unit
    def test_max_name_len_is_255(self):
        assert Validate.max_name_len == 255

    @pytest.mark.unit
    @pytest.mark.parametrize('fn,sep', [(Validate.snapshot, '@'), (Validate.bookmark, '#')])
    def test_snapshot_and_bookmark_length_boundary(self, fn, sep):
        fn('t' * 253 + sep + 'a')  # exactly 255
        with pytest.raises(ValueError):
            fn('t' * 254 + sep + 'a')  # 256

    @pytest.mark.unit
    def test_dataset_length_boundary(self):
        Validate.dataset('t' * 255)
        with pytest.raises(ValueError):
            Validate.dataset('t' * 256)
        Validate.dataset('t/' + 'a' * 253)
        with pytest.raises(ValueError):
            Validate.dataset('t/' + 'a' * 254)

    @pytest.mark.unit
    def test_snapshots_length_is_per_expanded_name(self):
        # The list as a whole is far over 255 characters, but every `t@short` is short.
        Validate.snapshots('t@' + ','.join(['a' * 200] * 5))
        # `t`*254 + '@' + 'a' is 256 characters once expanded.
        with pytest.raises(ValueError):
            Validate.snapshots('t' * 254 + '@a%bb')
        with pytest.raises(ValueError):
            Validate.snapshots('t' * 254 + '@a')
        Validate.snapshots('t' * 253 + '@a')
        Validate.snapshots('t' * 253 + '@a%')
        with pytest.raises(ValueError):
            Validate.snapshots('t' * 253 + '@a,bb')

    @pytest.mark.unit
    def test_from_name_and_constructors_enforce_length_and_dots(self):
        assert ZFS.from_name('t' * 253 + '@a', 'snapshot').short == 'a'
        assert ZFS.from_name('t' * 253 + '#a', 'bookmark').short == 'a'
        assert Filesystem('t' * 255).name == 't' * 255
        for name, kind in (('t' * 254 + '@a', 'snapshot'), ('t' * 254 + '#a', 'bookmark'),
                           ('t' * 256, 'filesystem'), ('t' * 256, 'volume'), ('pool@..', 'snapshot'),
                           ('pool#.', 'bookmark')):
            with pytest.raises(ValueError):
                ZFS.from_name(name, kind)
        with pytest.raises(ValueError):
            ZFS.from_name('t' * 254 + '@a')  # guessed type: nothing matches
        ds = Filesystem('t' * 253)
        assert Snapshot(ds, 'a').name == 't' * 253 + '@a'
        assert Bookmark(ds, 'a').name == 't' * 253 + '#a'
        with pytest.raises(ValueError):
            Snapshot(ds, 'bb')
        with pytest.raises(ValueError):
            Bookmark(ds, 'bb')
        with pytest.raises(ValueError):
            Snapshot(Filesystem('pool/ds'), '..')
        with pytest.raises(ValueError):
            Bookmark(Filesystem('pool/ds'), '.')

    @pytest.mark.unit
    def test_snapshot_range_enforces_list_grammar(self):
        with pytest.raises(ValueError):
            SnapshotRange.from_name('pool/ds@a%b%c')
        with pytest.raises(ValueError):
            SnapshotRange.from_name('pool/ds@..%b')


class TestValidatorsAreLinearTime:
    """ReDoS guard: a very long input that fails only at the very end must be rejected quickly by every validator."""

    N = 50_000
    LIMIT = 1.0

    @staticmethod
    def _time_reject(fn, s):
        import time
        start = time.perf_counter()
        with pytest.raises(ValueError):
            fn(s)
        return time.perf_counter() - start

    @pytest.mark.unit
    @pytest.mark.parametrize('filler', [' ', '-', 'a', 'a b-', '. :_'])
    def test_long_failing_inputs_reject_fast(self, filler):
        body = (filler * (self.N // len(filler) + 1))[:self.N]
        # Each candidate starts validly and fails only at the end (trailing '!' or '\n').
        for end in ('!', '\n', ' '):
            cases = [
                (Validate.dataset, 'pool/a' + body + end),
                (Validate.snapshot, 'pool/ds@a' + body + end),
                (Validate.snapshots, 'pool/ds@a' + body + '%a' + body + end),
                (Validate.bookmark, 'pool/ds#a' + body + end),
                (Validate.zfsname, 'a' + body + end),
                (Validate.attribute, 'a' + body + end),
            ]
            for fn, s in cases:
                elapsed = self._time_reject(fn, s)
                assert elapsed < self.LIMIT, '%s took %.2fs for filler %r end %r' % (fn.__name__, elapsed, filler, end)

    # The length cap now rejects inputs this long before any regex runs, so the tests below lift the cap
    # (monkeypatch Validate.max_name_len) and, in addition, call the compiled patterns directly. The regex itself is
    # therefore what is being timed.
    SEPARATOR_N = 20_000

    @staticmethod
    def _pattern_cases(n):
        yield 'snaps', Validate.pool + Validate.ds + '*@' + Validate.snaps, 'p@' + 'a%' * n + '!'
        yield 'snaps', Validate.pool + Validate.ds + '*@' + Validate.snaps, 'p@' + 'a,' * n + '!'
        yield 'snaps', Validate.pool + Validate.ds + '*@' + Validate.snaps, 'p@' + 'a b%' * n
        yield 'snaps', Validate.pool + Validate.ds + '*@' + Validate.snaps, 'p@' + 'a b,' * n + '!'
        yield 'snapshot', Validate.pool + Validate.ds + '*@' + Validate.short, 'p@' + 'a b' * n + ' '
        yield 'bookmark', Validate.pool + Validate.ds + '*#' + Validate.short, 'p#' + 'a b' * n + ' '
        yield 'dataset', Validate.pool + Validate.ds + '*', 'p' + '/a b' * n + ' '
        yield 'dataset', Validate.pool + Validate.ds + '*', 'p' + '/a b' * n + '!'

    @pytest.mark.unit
    def test_compiled_patterns_reject_separator_repeats_fast(self):
        import re
        import time
        for label, pattern, s in self._pattern_cases(self.SEPARATOR_N):
            start = time.perf_counter()
            assert re.fullmatch(pattern, s, re.ASCII) is None
            elapsed = time.perf_counter() - start
            assert elapsed < self.LIMIT, '%s took %.2fs for %r...' % (label, elapsed, s[:12])

    @pytest.mark.unit
    def test_validators_reject_separator_repeats_fast_with_cap_lifted(self, monkeypatch):
        monkeypatch.setattr(Validate, 'max_name_len', 10 ** 9)
        n = self.SEPARATOR_N
        cases = [
            (Validate.snapshots, 'p@' + 'a%' * n + '!'),
            (Validate.snapshots, 'p@' + 'a,' * n + '!'),
            (Validate.snapshots, 'p@' + 'a b%' * n),
            (Validate.snapshot, 'p@' + 'a b' * n + ' '),
            (Validate.bookmark, 'p#' + 'a b' * n + ' '),
            (Validate.dataset, 'p' + '/a b' * n + ' '),
        ]
        for fn, s in cases:
            elapsed = self._time_reject(fn, s)
            assert elapsed < self.LIMIT, '%s took %.2fs for %r...' % (fn.__name__, elapsed, s[:12])

    @pytest.mark.unit
    def test_separator_repeats_rejected_by_default_cap_too(self):
        # With the real cap the length check (or the regex, for `snapshots`, which has no whole-string cap) answers.
        n = self.SEPARATOR_N
        for fn, s in [(Validate.snapshots, 'p@' + 'a%' * n + '!'), (Validate.snapshots, 'p@' + 'a b%' * n),
                      (Validate.dataset, 'p' + '/a b' * n + ' ')]:
            assert self._time_reject(fn, s) < self.LIMIT


DEFAULT_QUOTA_NAMES = ['defaultuserquota', 'defaultgroupquota', 'defaultprojectquota', 'defaultuserobjquota',
                       'defaultgroupobjquota', 'defaultprojectobjquota']


class TestOpenZFS24DefaultQuotas:
    """The six OpenZFS 2.4 default-quota names are first-class filesystem properties; old indices are unchanged."""

    @pytest.mark.unit
    def test_appended_after_existing_names(self):
        assert PropertyNames.filesystem[30] == 'zoned'
        assert PropertyNames.filesystem[31:37] == DEFAULT_QUOTA_NAMES
        assert PropertyNames.mutable[33] == 'zoned'
        assert PropertyNames.mutable[34:40] == DEFAULT_QUOTA_NAMES

    @pytest.mark.unit
    def test_existing_indices_unchanged(self):
        assert PropertyNames.filesystem[0] == 'aclmode'
        assert PropertyNames.filesystem.index('quota') == 17
        assert PropertyNames.filesystem.index('xattr') == 29
        assert PropertyNames.mutable.index('aclinherit') == 0
        assert PropertyNames.mutable.index('quota') == 15
        assert PropertyNames.mutable.index('volsize') == 30
        assert PropertyNames.mutable.index('xattr') == 32
        assert PropertyNames.filesystem.index('zoned') == 30
        assert PropertyNames.mutable.index('zoned') == 33
        assert PropertyNames.mutable.index('defaultprojectobjquota') == 39

    @pytest.mark.unit
    @pytest.mark.parametrize('name', DEFAULT_QUOTA_NAMES)
    def test_constructor_indexes_name_and_keeps_source(self, name):
        fs = Filesystem('pool/fs', {name: Property('1G', 'local', '2G')})
        assert fs[name].value == '1G'
        assert fs[name].source == 'local'
        assert fs[name].received == '2G'
        assert getattr(fs, name).value == '1G'
        assert name in fs
        i = Filesystem._prop_names.index(name)
        assert fs._props[i].value == '1G'
        assert name not in fs._extra_props
        assert (name, fs._props[i]) in list(fs.properties)

    @pytest.mark.unit
    def test_plain_value_is_wrapped(self):
        assert Filesystem('pool/fs', {'defaultuserquota': '5G'})['defaultuserquota'].value == '5G'

    @pytest.mark.unit
    def test_native_attribute_and_attribute_accept_them(self):
        for name in DEFAULT_QUOTA_NAMES:
            Validate.attribute(name)
            Validate.native_attribute(name)


class TestNativeAttributeValidator:

    @pytest.mark.unit
    @pytest.mark.parametrize('name', ['futureprop', 'com.apple.foo', 'a:b', 'compression', 'x_1', 'a'])
    def test_accepts(self, name):
        Validate.native_attribute(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', ['Futureprop', '1abc', 'a-b', 'foo\n', 'fóo', 'ś', '', 'com.apple.', 'a b',
                                      'com.apple.Foo', 'com.other.foo', '_x'])
    def test_rejects(self, name):
        with pytest.raises(ValueError):
            Validate.native_attribute(name)

    @pytest.mark.unit
    def test_strict_attribute_still_rejects_unknown_native(self):
        with pytest.raises(ValueError):
            Validate.attribute('futureprop')


class TestExtraProperties:
    """Well-formed unknown native names are kept as extras, but only through update(..., lenient=True)."""

    @staticmethod
    def _fs():
        fs = Filesystem('pool/fs')
        fs.update({'futureprop': Property('x', 'local', '-')}, lenient=True)
        return fs

    @pytest.mark.unit
    def test_visible_through_every_accessor(self):
        fs = self._fs()
        assert fs['futureprop'].value == 'x'
        assert fs['futureprop'].source == 'local'
        assert fs.futureprop.value == 'x'
        assert 'futureprop' in fs
        assert ('futureprop', fs['futureprop']) in list(fs.properties)

    @pytest.mark.unit
    def test_not_stored_in_indexed_or_user_props(self):
        fs = self._fs()
        assert 'futureprop' not in fs._props.values()
        assert fs._props == {}
        assert fs._user_props == {}
        assert list(fs._extra_props) == ['futureprop']

    @pytest.mark.unit
    def test_plain_value_is_wrapped_and_overwritten(self):
        fs = Filesystem('pool/fs')
        fs.update({'futureprop': 'x'}, lenient=True)
        assert isinstance(fs['futureprop'], Property)
        fs.update({'futureprop': 'y'}, lenient=True)
        assert fs['futureprop'].value == 'y'

    @pytest.mark.unit
    def test_none_deletes(self):
        fs = self._fs()
        fs.update({'futureprop': None}, lenient=True)
        assert 'futureprop' not in fs._extra_props
        assert list(fs.properties) == []
        with pytest.raises(ValueError):
            fs['futureprop']
        # deleting an absent extra is a no-op
        fs.update({'futureprop': None}, lenient=True)

    @pytest.mark.unit
    def test_reset_clears_extras(self):
        fs = self._fs()
        fs.reset()
        assert fs._extra_props == {}
        assert list(fs.properties) == []

    @pytest.mark.unit
    def test_mixed_with_known_and_user_properties(self):
        fs = Filesystem('pool/fs')
        fs.update({'compression': 'lz4', 'org:k': 'v', 'futureprop': 'x'}, lenient=True)
        assert dict((k, v.value) for k, v in fs.properties) == {'compression': 'lz4', 'org:k': 'v', 'futureprop': 'x'}
        assert list(fs._user_props) == ['org:k']
        assert list(fs._extra_props) == ['futureprop']

    @pytest.mark.unit
    def test_known_name_of_another_type_lands_in_extras(self):
        fs = Filesystem('pool/fs')
        fs.update({'volsize': '1G'}, lenient=True)
        assert fs['volsize'].value == '1G'
        assert fs._props == {}

    @pytest.mark.unit
    def test_lenient_still_rejects_malformed_names(self):
        fs = Filesystem('pool/fs')
        for bad in ('Futureprop', '1abc', 'a-b', 'foo\n', 'fóo', ''):
            with pytest.raises(ValueError):
                fs.update({bad: 'v'}, lenient=True)
        assert fs._extra_props == {}

    @pytest.mark.unit
    def test_extras_work_on_every_type(self):
        for obj in (Dataset('pool/d'), Volume('pool/v'), Filesystem('pool/f')):
            obj.update({'futureprop': 'x'}, lenient=True)
            assert obj['futureprop'].value == 'x'
        snap = Snapshot(Filesystem('pool/f'), 's')
        snap.update({'futureprop': 'x'}, lenient=True)
        assert snap.futureprop.value == 'x'
        bm = Bookmark(Filesystem('pool/f'), 'b')
        bm.update({'futureprop': 'x'}, lenient=True)
        assert 'futureprop' in bm

    @pytest.mark.unit
    def test_pickle_and_deepcopy_keep_extras(self):
        fs = self._fs()
        for clone in (pickle.loads(pickle.dumps(fs)), copy.deepcopy(fs), copy.copy(fs)):
            assert clone['futureprop'].value == 'x'
            assert clone.futureprop.source == 'local'
            assert 'futureprop' in clone
            assert clone.name == 'pool/fs'

    @pytest.mark.unit
    def test_other_unknown_names_stay_strict_after_unpickle(self):
        clone = pickle.loads(pickle.dumps(self._fs()))
        with pytest.raises(ValueError):
            clone['otherprop']
        with pytest.raises(AttributeError):
            clone.otherprop


class TestStrictnessUnchangedForCallers:
    """Caller-supplied unknown native names are still refused everywhere."""

    @pytest.mark.unit
    def test_constructor_rejects(self):
        with pytest.raises(ValueError):
            Filesystem('pool/fs', {'futureprop': 'v'})

    @pytest.mark.unit
    def test_update_without_lenient_rejects(self):
        fs = Filesystem('pool/fs')
        with pytest.raises(ValueError):
            fs.update({'futureprop': 'v'})
        with pytest.raises(ValueError):
            fs.update({'futureprop': 'v'}, lenient=False)
        assert fs._extra_props == {}

    @pytest.mark.unit
    def test_update_none_without_lenient_rejects(self):
        with pytest.raises(ValueError):
            Filesystem('pool/fs').update({'futureprop': None})

    @pytest.mark.unit
    def test_accessors_on_object_without_extras_reject(self):
        fs = Filesystem('pool/fs')
        with pytest.raises(ValueError):
            fs['futureprop']
        with pytest.raises(ValueError):
            'futureprop' in fs
        with pytest.raises(AttributeError):
            fs.futureprop

    @pytest.mark.unit
    def test_strict_update_cannot_overwrite_an_extra(self):
        fs = Filesystem('pool/fs')
        fs.update({'futureprop': 'x'}, lenient=True)
        with pytest.raises(ValueError):
            fs.update({'futureprop': 'y'})
        assert fs['futureprop'].value == 'x'


class TestCheckProperty:
    """`ZFS.check_property`: strict, class-aware validation of a caller-supplied name that never modifies the object."""

    @pytest.mark.unit
    @pytest.mark.parametrize('name', ['compression', 'defaultuserquota', 'org:k', 'a:b', 'com.apple:x'])
    def test_valid_for_filesystem(self, name):
        Filesystem('pool/fs').check_property(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', ['futureprop', 'volblocksize', 'Compression', 'a b', '', 'compression\n', 'ś'])
    def test_invalid_for_filesystem(self, name):
        with pytest.raises(ValueError):
            Filesystem('pool/fs').check_property(name)

    @pytest.mark.unit
    def test_is_class_specific(self):
        Volume('pool/v').check_property('volblocksize')
        with pytest.raises(ValueError):
            Volume('pool/v').check_property('defaultuserquota')
        with pytest.raises(ValueError):
            Filesystem('pool/f').check_property('volblocksize')

    @pytest.mark.unit
    def test_does_not_accept_an_existing_extra(self):
        fs = Filesystem('pool/fs')
        fs.update({'futureprop': 'x'}, lenient=True)
        with pytest.raises(ValueError):
            fs.check_property('futureprop')

    @pytest.mark.unit
    def test_agrees_with_strict_update(self):
        for name in ['compression', 'futureprop', 'volblocksize', 'org:k', 'Bad']:
            fs = Filesystem('pool/fs')
            try:
                fs.update({name: 'v'})
                updated = True
            except ValueError:
                updated = False
            try:
                Filesystem('pool/fs').check_property(name)
                checked = True
            except ValueError:
                checked = False
            assert checked == updated, name

    @pytest.mark.unit
    def test_has_no_side_effects(self):
        fs = Filesystem('pool/fs')
        fs.check_property('compression')
        assert fs._props == {} and fs._user_props == {} and fs._extra_props == {}


class TestObjectsWithoutExtraPropsAttribute:
    """Objects unpickled from before `_extra_props` existed must keep working (the dict is created on demand)."""

    @staticmethod
    def _legacy():
        """What unpickling an old pickle produces: __dict__ restored without `_extra_props` (no __init__ runs)."""
        src = Filesystem('pool/fs')
        src.update({'compression': 'lz4', 'org:k': 'v'})
        state = {k: v for k, v in src.__dict__.items() if k != '_extra_props'}
        obj = Filesystem.__new__(Filesystem)
        obj.__dict__.update(state)
        return obj

    @pytest.mark.unit
    def test_legacy_object_really_lacks_the_attribute(self):
        assert '_extra_props' not in self._legacy().__dict__

    @pytest.mark.unit
    def test_properties_getitem_contains(self):
        fs = self._legacy()
        assert dict((k, v.value) for k, v in fs.properties) == {'compression': 'lz4', 'org:k': 'v'}
        assert fs['compression'].value == 'lz4'
        assert fs['atime'] is None
        assert 'compression' in fs
        assert 'atime' not in fs
        assert fs.compression.value == 'lz4'
        with pytest.raises(ValueError):
            fs['futureprop']
        with pytest.raises(ValueError):
            'futureprop' in fs
        with pytest.raises(AttributeError):
            fs.futureprop

    @pytest.mark.unit
    def test_lenient_update_creates_extras_on_demand(self):
        fs = self._legacy()
        fs.update({'futureprop': 'x'}, lenient=True)
        assert fs['futureprop'].value == 'x'
        assert 'futureprop' in fs
        assert fs._extra_props == {'futureprop': fs['futureprop']}
        fs.update({'futureprop': None}, lenient=True)
        assert fs._extra_props == {}

    @pytest.mark.unit
    def test_strict_update_and_reset_and_check_property(self):
        fs = self._legacy()
        with pytest.raises(ValueError):
            fs.update({'futureprop': 'x'})
        fs.check_property('atime')
        fs.reset()
        assert list(fs.properties) == []

    @pytest.mark.unit
    @pytest.mark.parametrize('make', [lambda: Volume('pool/v'), lambda: Dataset('pool/d'),
                                      lambda: Snapshot(Filesystem('pool/f'), 's'),
                                      lambda: Bookmark(Filesystem('pool/f'), 'b')])
    def test_other_types(self, make):
        obj = make()
        del obj.__dict__['_extra_props']
        assert list(obj.properties) == []
        obj.update({'futureprop': 'x'}, lenient=True)
        assert obj['futureprop'].value == 'x'


class TestInvalidNameError:
    """Name-shape failures raise InvalidNameError (a ValueError); other validators keep raising plain ValueError."""

    BAD_DATASETS = ['pool/.system', 'pool/-x', 'pool/:x', 'pool/a ', 'pool/ a', '', '1pool/fs', 'pool//fs']
    BAD_SNAPSHOTS = ['pool/fs@ manual', 'pool/fs@manual ', 'pool/.system@snap', 'pool/fs@', 'pool/fs@.', 'pool/fs@..',
                     'pool/fs']
    BAD_BOOKMARKS = ['pool/-x#bm', 'pool/fs# bm', 'pool/fs#bm ', 'pool/fs#.', 'pool/fs#..', 'pool/fs']

    @pytest.mark.unit
    @pytest.mark.parametrize('name', BAD_DATASETS)
    def test_dataset(self, name):
        with pytest.raises(InvalidNameError):
            Validate.dataset(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', BAD_SNAPSHOTS)
    def test_snapshot(self, name):
        with pytest.raises(InvalidNameError):
            Validate.snapshot(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', ['pool/fs@ manual', 'pool/.system@snap', 'pool/fs@.', 'pool/fs@a,..', 'pool/fs@%',
                                      'pool/fs@a%b%c', 'pool/fs@'])
    def test_snapshots(self, name):
        with pytest.raises(InvalidNameError):
            Validate.snapshots(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', BAD_BOOKMARKS)
    def test_bookmark(self, name):
        with pytest.raises(InvalidNameError):
            Validate.bookmark(name)

    @pytest.mark.unit
    def test_length_over_255(self):
        long_ds = 'pool/' + 'a' * 251                      # 256 characters
        assert len(long_ds) == 256
        with pytest.raises(InvalidNameError):
            Validate.dataset(long_ds)
        with pytest.raises(InvalidNameError):
            Validate.snapshot('pool/fs@' + 'a' * 250)
        with pytest.raises(InvalidNameError):
            Validate.bookmark('pool/fs#' + 'a' * 250)
        with pytest.raises(InvalidNameError):
            Validate.snapshots('pool/fs@' + 'a' * 250)
        Validate.dataset('pool/' + 'a' * 250)             # exactly 255 is fine

    @pytest.mark.unit
    @pytest.mark.parametrize('validator, name', [
        (Validate.snapshot, 'pool/fs@.'), (Validate.snapshot, 'pool/fs@..'),
        (Validate.snapshots, 'pool/fs@a,.'), (Validate.snapshots, 'pool/fs@a%..'),
        (Validate.bookmark, 'pool/fs#.'), (Validate.bookmark, 'pool/fs#..'),
    ])
    def test_dots_checks(self, validator, name):
        with pytest.raises(InvalidNameError):
            validator(name)

    @pytest.mark.unit
    def test_is_a_value_error(self):
        assert issubclass(InvalidNameError, ValueError)
        with pytest.raises(ValueError):
            Validate.dataset('pool/.system')

    @pytest.mark.unit
    @pytest.mark.parametrize('validator, value', [
        (Validate.attribute, 'not_a_property'), (Validate.attribute, 'bad name!'),
        (Validate.type, 'bogus'), (Validate.source, 'bogus'), (Validate.zfsname, 'bad name!'),
        (Validate.propfield, 'bogus'),
    ])
    def test_other_validators_are_plain_value_errors(self, validator, value):
        with pytest.raises(ValueError) as info:
            validator(value)
        assert not isinstance(info.value, InvalidNameError)

    @pytest.mark.unit
    def test_valid_names_do_not_raise(self):
        Validate.dataset('pool/fs')
        Validate.dataset('pool/a b')
        Validate.snapshot('pool/fs@snap')
        Validate.snapshot('pool/fs@a b')
        Validate.snapshots('pool/fs@a,b%c')
        Validate.bookmark('pool/fs#bm')

    @pytest.mark.unit
    @pytest.mark.parametrize('cls', [Dataset, Filesystem, Volume])
    @pytest.mark.parametrize('name', ['tank/.system', 'pool/-x', 'pool/:x', 'pool/a ', 'pool/ a'])
    def test_constructors_raise_it(self, cls, name):
        with pytest.raises(InvalidNameError):
            cls(name)

    @pytest.mark.unit
    def test_snapshot_and_bookmark_constructors_raise_it(self):
        fs = Filesystem('pool/fs')
        with pytest.raises(InvalidNameError):
            Snapshot(fs, ' manual')
        with pytest.raises(InvalidNameError):
            Snapshot(fs, 'manual ')
        with pytest.raises(InvalidNameError):
            Bookmark(fs, '..')

    @pytest.mark.unit
    @pytest.mark.parametrize('name, dstype', [
        ('tank/.system', 'filesystem'), ('tank/.system', 'volume'), ('tank/.system', 'dataset'),
        ('pool/-x', 'filesystem'), ('pool/:x', 'filesystem'), ('pool/a ', 'filesystem'), ('pool/ a', 'filesystem'),
        ('pool/fs@ manual', 'snapshot'), ('pool/fs@manual ', 'snapshot'), ('pool/.system@snap', 'snapshot'),
        ('pool/-x#bm', 'bookmark'), ('pool/fs# bm', 'bookmark'),
    ])
    def test_from_name_raises_it(self, name, dstype):
        with pytest.raises(InvalidNameError):
            ZFS.from_name(name, dstype)

    @pytest.mark.unit
    def test_from_name_with_a_bad_type_is_not_an_invalid_name(self):
        with pytest.raises(ValueError, match='not a valid ZFS type') as info:
            ZFS.from_name('pool/fs', 'bogus')
        assert not isinstance(info.value, InvalidNameError)

    @pytest.mark.unit
    def test_from_name_valid(self):
        assert isinstance(ZFS.from_name('pool/fs', 'filesystem'), Filesystem)
        assert isinstance(ZFS.from_name('pool/fs@snap', 'snapshot'), Snapshot)
        assert isinstance(ZFS.from_name('pool/fs#bm', 'bookmark'), Bookmark)


class TestPropertyNamesAdditions:
    """Native properties added to the per-type tables; each table only ever grows at its end."""

    VOLUME_NAMES = ['prefetch', 'special_small_blocks', 'volthreading']
    SNAPSHOT_NAMES = ['encryptionroot', 'keystatus']

    @staticmethod
    def _names(obj):
        return list(obj._prop_names)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', ['receive_resume_token'] + VOLUME_NAMES + SNAPSHOT_NAMES + DEFAULT_QUOTA_NAMES)
    def test_new_names_are_known(self, name):
        assert name in PropertyNames.all
        Validate.attribute(name)

    @pytest.mark.unit
    def test_receive_resume_token_is_a_read_only_filesystem_and_volume_property(self):
        assert PropertyNames.filesystem[-1] == 'receive_resume_token'
        assert PropertyNames.volume[-1] == 'receive_resume_token'
        assert 'receive_resume_token' not in PropertyNames.mutable
        for table in (PropertyNames.snapshot, PropertyNames.fs_snap, PropertyNames.vol_snap,
                      PropertyNames.bookmark, PropertyNames.dataset):
            assert 'receive_resume_token' not in table
        assert PropertyNames.all.count('receive_resume_token') == 2

    @pytest.mark.unit
    @pytest.mark.parametrize('cls', [Filesystem, Volume])
    def test_filesystem_and_volume_accept_receive_resume_token(self, cls):
        token = '1-c0858fc6f-c0-789c'
        obj = cls('pool/x', {'receive_resume_token': Property(token)})
        assert obj['receive_resume_token'].value == token
        assert 'receive_resume_token' in self._names(obj)
        assert 'receive_resume_token' not in obj._extra_props
        obj.check_property('receive_resume_token')
        assert cls('pool/y', {'receive_resume_token': token})['receive_resume_token'].value == token

    @pytest.mark.unit
    def test_receive_resume_token_is_not_a_snapshot_or_bookmark_property(self):
        for obj in (Snapshot(Filesystem('pool/fs'), 's'), Snapshot(Volume('pool/v'), 's'),
                    Bookmark(Filesystem('pool/fs'), 'b'), Dataset('pool/d')):
            with pytest.raises(ValueError):
                obj.check_property('receive_resume_token')

    @pytest.mark.unit
    def test_receive_resume_token_is_never_settable_by_name_in_the_mutable_table(self):
        assert 'receive_resume_token' not in PropertyNames.mutable
        assert PropertyNames.mutable[-1] == 'volthreading'

    @pytest.mark.unit
    @pytest.mark.parametrize('name', VOLUME_NAMES)
    def test_volume_accepts_the_new_names(self, name):
        assert name in PropertyNames.volume
        vol = Volume('pool/v', {name: Property('x', 'local')})
        assert vol[name].value == 'x'
        assert name in self._names(vol)
        assert name not in vol._extra_props
        vol.check_property(name)

    @pytest.mark.unit
    def test_only_volthreading_is_newly_mutable(self):
        assert PropertyNames.mutable[-1] == 'volthreading'
        assert PropertyNames.mutable.count('volthreading') == 1

    @pytest.mark.unit
    @pytest.mark.parametrize('name', SNAPSHOT_NAMES)
    @pytest.mark.parametrize('ds', [Filesystem('pool/fs'), Volume('pool/v'), Dataset('pool/d')])
    def test_every_snapshot_accepts_encryptionroot_and_keystatus(self, name, ds):
        assert name in PropertyNames.snapshot
        snap = Snapshot(ds, 's', {name: Property('pool/enc')})
        assert snap[name].value == 'pool/enc'
        assert name in self._names(snap)
        snap.check_property(name)

    @pytest.mark.unit
    @pytest.mark.parametrize('name', DEFAULT_QUOTA_NAMES)
    def test_filesystem_snapshot_accepts_the_default_quotas(self, name):
        assert name in PropertyNames.fs_snap
        snap = Snapshot(Filesystem('pool/fs'), 's', {name: Property('5G')})
        assert snap[name].value == '5G'
        assert name in self._names(snap)
        assert name in self._names(Snapshot(Dataset('pool/d'), 's'))

    @pytest.mark.unit
    @pytest.mark.parametrize('name', DEFAULT_QUOTA_NAMES)
    def test_volume_snapshot_rejects_the_default_quotas(self, name):
        assert name not in self._names(Snapshot(Volume('pool/v'), 's'))
        with pytest.raises(ValueError):
            Snapshot(Volume('pool/v'), 's').check_property(name)

    @pytest.mark.unit
    def test_vol_snap_table(self):
        assert PropertyNames.vol_snap == ['volsize', 'prefetch']

    @pytest.mark.unit
    def test_volume_snapshot_accepts_volsize_and_prefetch(self):
        snap = Snapshot(Volume('pool/v'), 's', {'volsize': Property('1G'), 'prefetch': Property('all')})
        assert snap['volsize'].value == '1G'
        assert snap['prefetch'].value == 'all'

    @pytest.mark.unit
    def test_prefetch_is_also_a_filesystem_snapshot_property(self):
        assert 'prefetch' in PropertyNames.fs_snap
        assert Snapshot(Filesystem('pool/fs'), 's', {'prefetch': 'all'})['prefetch'].value == 'all'

    @pytest.mark.unit
    def test_snapshots_still_reject_unrelated_names(self):
        with pytest.raises(ValueError):
            Snapshot(Volume('pool/v'), 's').check_property('xattr')
        with pytest.raises(ValueError):
            Snapshot(Filesystem('pool/fs'), 's').check_property('volsize')
