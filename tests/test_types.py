"""Unit tests for libzfseasy.types module."""

import pytest
from libzfseasy.types import (
    Validate, Property, PropertyNames, ZFS, Dataset, Filesystem, 
    Volume, Snapshot, SnapshotRange, Bookmark
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
