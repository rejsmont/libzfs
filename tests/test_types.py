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
