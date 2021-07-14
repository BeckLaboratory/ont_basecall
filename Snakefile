"""
Re-basecall ONT runs from FAST5.
"""

### Libraries ###

import Bio.bgzf
import Bio.SeqIO
import gzip
import numpy as np
import os
import pandas as pd
import shutil

import ontbclib


### Get install directory ###

INSTALL_DIR = os.path.dirname(workflow.snakefile)


### Load config ###

CONFIG_FILE_NAME = 'config/config.json'
CONFIG_FILE_NAME_INSTALLED = os.path.join(INSTALL_DIR, CONFIG_FILE_NAME)

if os.path.isfile(CONFIG_FILE_NAME_INSTALLED):
    configfile: CONFIG_FILE_NAME_INSTALLED

configfile: CONFIG_FILE_NAME


### Setup cell table ###

CELL_TABLE_FILE_NAME = config.get('cell_table', 'config/cells.tsv')

CELL_TABLE = ontbclib.rules.get_cell_table(CELL_TABLE_FILE_NAME)


### Shell prefix ###

SHELL_PREFIX = 'set -euo pipefail; '

SETENV_SITE = f'{INSTALL_DIR}/config/setenv.sh'
SETENV_LOCAL = 'config/setenv.sh'

if os.path.isfile(SETENV_SITE):
    SHELL_PREFIX += f'source {SETENV_SITE}; '

if os.path.isfile(SETENV_LOCAL):
    SHELL_PREFIX += f'source {SETENV_LOCAL}; '

shell.prefix(SHELL_PREFIX)


### Includes ###

include: 'rules/basecall.snakefile'
