# ONT basecalling pipeline #

Input FAST5 files, output bgzipped and indexed FASTQ files.


## Setup ##


### Configuration JSON ###

The pipeline will read `config/config.json` relative to the run directory
and the pipeline install directory (run directory takes precedence).

Parameters:
* profile: A dictionary of profiles. See "Profiles" below.
* tempdir: Create a subdirectory here. A subdirectory (cell name)
  will be created at this location
* fast5_dir: FAST5 input directory pattern with `{cell}` wildcard. Used
  to located FAST5 files if there is not an explicit path in the
  cell configuration.
* default_profile: Name of the profile to use if not explicitly configured.

#### Profiles ####

In the `profile` section, each entry is keyed by the profile name and has a set
of parameters under it.

Profile options:
* guppy_dir: Directory where Guppy is installed. `bin/guppy_basecaller` should be
  found in this directory.
* cfg: Name of basecall config built into Guppy to run (e.g. `dna_r9.4.1_450bps_sup_prom.cfg`).
  The contents of this value are passed to `guppy_basecaller` with `-c` on the command-line.


#### Example ####

Example configuration:

```
{
    "profile": {
        "5.0.11": {
            "guppy_dir": "/path/to/guppy_5.0.11/ont-guppy",
            "cfg": "dna_r9.4.1_450bps_sup_prom.cfg"
        }
    },
    "tempdir": "/path/to/temp",
    "fast5_dir": "/path/to/fast5/{cell}",
    "default_profile": "5.0.11"
}
```

In this configuration, both `default_profile` and `fast5_dir` are defined so that the
pipeline can run cells if they are not configured in the cell TSV (optional if defined)
in the cell TSV for every entry). Then default profile is "5.0.11", which runs Guppy
5.0.11 (from location on disk defined in `guppy_dir`) using the pre-defined configuration
`dna_r9.4.1_450bps_sup_prom.cfg` that was distributed with Guppy.


### Cell TSV ###

A TSV file with cell configurations in it.

The TSV is optional or may be incomplete (not have a record for every cell) if
`default_profile` and `fast5_dir` are defined in the config.

Fields:
* CELL: Cell name.
* FAST5_DIR: Directory for FAST5. Optional if `fast5dir` is defined in the config.
* PROFILE: Guppy version. Optional if `guppy_default` is defined in the config.

Other fields may be added, such as `COMMENTS`. `FAST5_DIR` and `VERSION` are optional
if `default_profile` and `fast5_dir` are defined in the config. The field header must
be present.


### Shell setenv.sh ###

First, the pipeline sets Bash strict mode `set -euo pipefail` for all shell rules.

If `config/setenv.sh` is found in the run directory or the pipeline install directory
(where `Snakefile` is found), it is sourced before any commands are run in any `shell`
rule. If both are found, both are sourced.
