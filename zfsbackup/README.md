# ZFS Backup Daemon

An automated ZFS snapshot management daemon with configurable frequency and retention policies.

## Features

- 🕐 **Automated Snapshots**: Take snapshots at configurable intervals (minutes, hours, days)
- 📋 **Retention Policies**: Define tiered retention rules based on snapshot age
- 🔄 **Recursive Snapshots**: Support for recursive dataset snapshots
- 🗑️ **Automatic Cleanup**: Automatically purge expired snapshots based on retention rules
- 🔍 **Dry Run Mode**: Test configuration without making changes
- 📊 **Detailed Logging**: Comprehensive logging of all operations
- ⚙️ **YAML Configuration**: Easy-to-read and maintain configuration files

## Installation

The daemon requires:
- Python 3.7+
- PyYAML
- ZFS utilities (zfs, zpool)
- The `libzfseasy` library (included in this project)

Install dependencies:
```bash
pip install PyYAML
```

## Configuration

Create a configuration file (default location: `/etc/zfsbackup/config.yaml`):

```yaml
snapshot_prefix: "autosnap"
check_interval: "5m"
dry_run: false

datasets:
  - name: "tank/data"
    enabled: true
    frequency: "1h"
    recursive: false
    retention:
      "1d": "1M"    # Keep daily snapshots for 1 month
      "1w": "3M"    # Keep weekly snapshots for 3 months
      "1M": "1y"    # Keep monthly snapshots for 1 year
```

See [config.example.yaml](config.example.yaml) for more examples.

## Usage

### Start the Daemon

```bash
# Using default config location
python -m zfsbackup.daemon

# With custom config
python -m zfsbackup.daemon -c /path/to/config.yaml

# Dry run mode (no changes made)
python -m zfsbackup.daemon --dry-run

# Verbose logging
python -m zfsbackup.daemon -v
```

### Test Configuration

```bash
python -m zfsbackup.daemon --test-config -c config.yaml
```

### Command Line Options

- `-c, --config PATH`: Path to configuration file (default: `/etc/zfsbackup/config.yaml`)
- `-d, --dry-run`: Dry run mode - do not create or destroy snapshots
- `-v, --verbose`: Enable verbose (debug) logging
- `--test-config`: Test configuration and exit

## Configuration Reference

### Global Settings

- `snapshot_prefix`: Prefix for all managed snapshots (default: "autosnap")
- `check_interval`: How often to check if snapshots are needed (e.g., "5m", "1h")
- `dry_run`: Set to `true` to test without making changes (default: false)

### Dataset Configuration

Each dataset can have:

- `name`: ZFS dataset path (required)
- `enabled`: Whether this dataset should be backed up (default: true)
- `frequency`: How often to take snapshots (default: "1h")
- `recursive`: Whether to snapshot child datasets (default: false)
- `retention`: Dictionary of retention rules

### Retention Rules

Retention rules define how long to keep snapshots based on their age:

```yaml
retention:
  "1d": "1M"    # Snapshots 1 day old: keep for 1 month
  "1w": "3M"    # Snapshots 1 week old: keep for 3 months
  "1M": "10y"   # Snapshots 1 month old: keep for 10 years
```

**How it works:**
- The daemon evaluates each snapshot's age
- Applies the appropriate retention rule
- Deletes snapshots when: `snapshot_age + keep_duration > now`

### Time Format

- `h` = hours (e.g., "1h", "12h")
- `d` = days (e.g., "1d", "7d")
- `w` = weeks (e.g., "1w", "2w")
- `M` = months (e.g., "1M", "6M") - approximate as 30 days
- `y` = years (e.g., "1y", "10y") - approximate as 365 days

## Snapshot Naming

Snapshots are created with the format:
```
{prefix}_{YYYYMMDD}_{HHMMSS}
```

Example: `autosnap_20231215_143022`

This naming convention allows the daemon to:
- Identify managed snapshots
- Parse creation timestamps
- Calculate snapshot age for retention policies

## Example Use Cases

### 1. Frequent Snapshots with Tiered Retention

For important data that changes frequently:

```yaml
- name: "tank/database"
  frequency: "15m"
  retention:
    "1h": "1d"     # Hourly granularity for 1 day
    "1d": "1w"     # Daily granularity for 1 week
    "1w": "1M"     # Weekly granularity for 1 month
    "1M": "1y"     # Monthly granularity for 1 year
```

### 2. Simple Daily Backups

For less critical data:

```yaml
- name: "tank/documents"
  frequency: "1d"
  retention:
    "1w": "1M"     # Keep for 1 month
    "1M": "1y"     # Then keep monthly for 1 year
```

### 3. Long-Term Archives

For data requiring long retention:

```yaml
- name: "tank/archives"
  frequency: "1w"
  retention:
    "1M": "10y"    # Keep monthly snapshots for 10 years
```

## Running as a Service

### systemd Service

Create `/etc/systemd/system/zfsbackup.service`:

```ini
[Unit]
Description=ZFS Backup Daemon
After=zfs-import.target

[Service]
Type=simple
ExecStart=/usr/bin/python3 -m zfsbackup.daemon -c /etc/zfsbackup/config.yaml
Restart=on-failure
RestartSec=60

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable zfsbackup
sudo systemctl start zfsbackup
sudo systemctl status zfsbackup
```

## Logging

The daemon logs to stdout by default. Key log messages include:
- Snapshot creation and deletion
- Retention policy application
- Errors and warnings
- Daemon lifecycle events

Use `-v` flag for debug-level logging.

## Safety Features

- **Dry Run Mode**: Test configuration without making changes
- **Prefix Filtering**: Only manages snapshots with the configured prefix
- **Graceful Shutdown**: Handles SIGINT/SIGTERM signals
- **Error Handling**: Continues operation even if individual datasets fail
- **Validation**: Validates configuration on startup

## Troubleshooting

### Snapshots not being created

1. Check dataset exists: `zfs list tank/data`
2. Check permissions: Run with appropriate ZFS permissions
3. Enable verbose logging: `python -m zfsbackup.daemon -v`
4. Test config: `python -m zfsbackup.daemon --test-config`

### Snapshots not being deleted

1. Check retention rules are correctly configured
2. Verify snapshot ages with: `zfs list -t snapshot -o name,creation`
3. Run in dry-run mode to see what would be deleted
4. Check logs for errors

### Configuration errors

```bash
# Test configuration
python -m zfsbackup.daemon --test-config -c config.yaml
```

## License

See the main project LICENSE file.

## Contributing

Contributions are welcome! Please ensure:
- Code follows existing style
- Add tests for new features
- Update documentation
