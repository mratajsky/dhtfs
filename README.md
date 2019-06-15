# dhtfs

This package provides the `dhtfs` utility and the `dhtfs-peer` and `dhtfs-spawn-peers` scripts for running DHT nodes.

The software was developed on Debian Buster and also (in a limited way) tested on Ubuntu 19.04.

## Dependencies

* Python 3.7

## Installation

The package includes a Pipfile, which describes the necessary Python dependencies. The easiest way to install/use it is through pipenv.

Example using virtual environment:

```
$ pipenv shell
$ pipenv install -e .
```

Example of local installation which installs required packages and places scripts into `.local/bin`:

```
$ pipenv install --system
```

## Usage

The `dhtfs` utility is a wrapper around `dhtfs-fuse` and mainly simplifies mounting DHTFS. It requires at least one running DHT node and a file system recorded in the DHT.

Example usage:
```
$ dhtfs-spawn-peers -t leveldb 1      # Run a single local DHT peer at default port
$ mkdir mnt                           # Create a mount point
$ dhtfs create                        # Create a file system
$ dhtfs mount -c /path/to/cache mnt   # Mount the file system
```

All scripts support command-line arguments and show a help screen when run with the `-h` switch. Additionally, dhtfs accepts a command, which can be followed by extra command-specific arguments. Each command also has a help screen, see e.g. `dhtfs mount -h`.

The `dhtfs-spawn-peers` script runs a chosen number of DHT nodes and bootstraps the DHT network. By default, it requires a running redis server, the `-t leveldb` makes it use a local LevelDB based database instead.

The `dhtfs create` command records a file system named `default` in the DHT with default parameters.

The `dhtfs mount` command is a wrapper around `dhtfs-fuse`, which must be installed. It requires a BTRFS volume for caching. If the home of the current user is on a BTRFS file system, it will automatically create a subvolume for its own use in `.cache/dhtfs`. Otherwise, the `-c` option must be used with path to a directory which is on a BTRFS volume/subvolume.

### Accessing earlier states

To mount an earlier state of the file system, see the `--at` option of `dhtfs mount`. This option can be passed time parseable by the [dateutil](https://dateutil.readthedocs.io/en/stable/parser.html) parser and works well with the default output of `date(1)`.

Example: `dhtfs mount -c /path/to/cache --at "Sat Jun 15 12:58:20 UTC 2019" mnt`.
