"""
File and directory utilities.
"""

import os
import tempfile

import ontbclib


def get_temp_path(cell_entry, config, prefix='ont_basecall/', suffix='', exists_ok=False):
    """
    Get path to a temporary directory (or file). This function does not create the directory, it just finds a path.

    if "tempdir" is defined in the configuration file, it is used and shell variables are expanded. If it does
    not exist in the config, `tempdir.gettempdir()` determines the temporary directory. Appended to this directory is
    "/{prefix}{cell}{suffix}" where "cell" comes from `cell_entry` and `prefix` and `suffix` come from the function call.

    :param cell_entry: Cell entry.
    :param config: Pipeline config.
    :param prefix: Prefix appended to the temporary path (before cell).
    :param suffix: Suffix appended to the temporary path (after cell).
    :param exists_ok: Fail if the path exists and this is not `True`.

    :return: Path to a temporary directory or file.
    """

    if 'tempdir' in config.keys():
        temp_dir = ontbclib.util.shell_expand(config['tempdir'])
    else:
        temp_dir = tempfile.gettempdir()

    cell = cell_entry['cell']

    temp_dir = os.path.join(temp_dir, f'{prefix}{cell}{suffix}')

    if os.path.exists(temp_dir) and not exists_ok:
        raise RuntimeError(f'Temporary direcotory exists: {temp_dir}')

    return temp_dir
