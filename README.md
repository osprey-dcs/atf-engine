# Quartz Acqusition Control Engine

The atf-engine coordinates the process of recording data
to file from a set of Osprey Quartz digitizer chassis.

Functions:

- Collect acquisition meta-data (eg. scaling factors, channel labels, etc.)
- Create filesystem structure for data files
- Configure individual Quartz IOCs to write appropriately named files
- Coordinate starting and stopping of acqusition
- Trigger post-processing with FileConverter to prepare for use of the Quartz previewer
- Write final acqusition meta-data `.hdr` file.

## Requires

- Python 3
- P4P
- FileConverter component of [Quartz previewer application](https://github.com/osprey-dcs/atf-previewer)

## Setup

Must be run with access to the same filesystem as the Quartz IOCs.

```sh
git clone https://github.com/osprey-dcs/atf-engine.git
virtualenv engine_env
./engine_env/bin/pip install -r  atf-engine/requirements.txt
```

## Running

The following is an example.
Prefer [`atf-engine.service`](atf-engine.service) for a real installation.

```sh
cd atf-engine
../engine_env/bin/python -m atf_engine \
 --root /data \
 --fileConverter ../atf-previewer/build/fileReformatter2/FileReformatter2
```

The instance will create each recording in time stamped sub-directory of `/data`.

## Manual post-processing

In the event that automatic post-processing needs to be repeated.
The following will read `original.hdr` to find `.dat` files.
It will then run the FileConverter to produce `.j` files in a location
relative to the output `updated.hdr`.

```sh
cd atf-engine
../engine_env/bin/python -m atf_engine.convert \
 --fileConverter ../atf-previewer/build/fileReformatter2/FileReformatter2 \
 /data/.../original.hdr \
 /data/.../updated.hdr
```

The output `.hdr` file location need not be in the same directory as the input.
Also, the input and output `.hdr` filenames may be the same, in which case
the input file will be overwitten on success.

However, it is recommended that input and output `.hdr` file names differ,
and be placed in the same directory.
