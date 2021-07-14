"""
Snakemake rule support
"""

import numpy as np
import os
import pandas as pd


def get_cell_table(cell_table_file_name):
    """
    Read the cell table as a DataFrame, check format, and apply default values.

    :param cell_table_file_name: Cell table file name. If missing, a default empty table is returned.

    :return: Cell table DataFrame.
    """

    if os.path.isfile(cell_table_file_name):

        # Read
        if cell_table_file_name.endswith('.tsv') or cell_table_file_name.endswith('.tsv.gz'):
            cell_table = pd.read_csv(cell_table_file_name, sep='\t', header=0)
        if cell_table_file_name.endswith('.xlsx'):
            cell_table = pd.read_excel(cell_table_file_name, header=0)
        else:
            raise RuntimeError(f'Cell table is not a TSV or XLSX: {cell_table_file_name}')

        # Error on missing columns
        missing_columns = [col for col in ('SAMPLE', 'CELL') if col not in cell_table.columns]

        if missing_columns:
            raise RuntimeError('Missing cell table columns: {}'.format(', '.join(missing_columns)))

        if 'FAST5_DIR' not in cell_table.columns:
            cell_table['FAST5_DIR'] = np.nan

        if 'PROFILE' not in cell_table.columns:
            cell_table['PROFILE'] = np.nan

        cell_table.set_index('CELL', inplace=True, drop=False)

    else:
        cell_table = pd.DataFrame(
            [], columns=['CELL', 'FAST5_DIR', 'PROFILE']
        ).set_index(
            'CELL', drop=False
        )

    return cell_table


def get_cell_entry(wildcards, cell_table, config):
    """
    Get a cell entry dictionary with cell name, FAST5 input path, profile name, etc.

    * fast5_dir: FAST5 input directory.
    * profile: Profile name (guppy path and config).
    * guppy_dir: Guppy install directory from the matching profile.
    * cfg: Guppy config from the matching profile.
    * profile_dict: Full profile dictionary.

    :param wildcards: Rule wildcards.
    :param cell_table: Cell table.
    :param config: Pipeline config.

    :return: Dictionary describing all the parameters needed to basecall this cell.
    """

    # Get cell
    if 'cell' not in wildcards.keys():
        raise RuntimeError('Cannot get cell entry: Missing "cell" wildcard')

    cell = wildcards.get('cell')

    # Init entry
    cell_entry = dict()

    cell_entry['cell'] = cell

    # Get cell row
    if cell not in cell_table.index:
        raise RuntimeError(f'Cell not in cell table: {cell}')

    df_cell = cell_table.loc[cell]

    cell_entry['fast5_dir'] = (
        df_cell['FAST5_DIR'] if not pd.isnull(df_cell['FAST5_DIR']) else None
    )

    cell_entry['profile'] = (
        df_cell['PROFILE'] if not pd.isnull(df_cell['PROFILE']) else None
    )

    cell_entry['sample'] = df_cell['SAMPLE']

    # Get FAST5 directory
    if cell_entry['fast5_dir'] is None:
        if 'fast5_dir' not in config:
            raise RuntimeError(f'No FAST5_DIR default for cell: {cell}')

        if '{cell}' not in config['fast5_dir']:
            raise RuntimeError(f'Default FAST5_DIR is missing wildcard "{{cell}}": {cell}')

        cell_entry['fast5_dir'] = config['fast5_dir'].format(cell=cell)

    # Process profile
    if cell_entry['profile'] is None:
        if 'default_profile' not in config:
            raise RuntimeError(f'No PROFILE default for cell: {cell}')

        cell_entry['profile'] = config['default_profile']

    # Get profile entry
    if 'profile' not in config:
        raise RuntimeError('No "profile" section in config')

    if cell_entry['profile'] not in config['profile']:
        raise RuntimeError('No profile in configuration section with name "{profile}": cell "{cell}"'.format(
            **cell_entry
        ))

    cell_profile = dict(config['profile'][cell_entry['profile']])

    if 'guppy_dir' not in cell_profile:
        raise RuntimeError('Profile "{profile}" is missing "guppy_dir" entry: cell "{cell}"'.format(
            **cell_entry
        ))

    if 'cfg' not in cell_profile:
        raise RuntimeError('Profile "{profile}" is missing "cfg" entry: cell "{cell}"'.format(
            **cell_entry
        ))

    cell_entry['guppy_dir'] = cell_profile['guppy_dir']
    cell_entry['cfg'] = cell_profile['cfg']
    cell_entry['profile_dict'] = cell_profile

    # Return entry
    return cell_entry

