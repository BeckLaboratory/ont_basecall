"""
Generally useful utilities.
"""

import io
import os
import pandas as pd
import re
import subprocess


def shell_expand(path, var_dict=None):
    """
    Expand path substituting environment variables ("$VAR" or "${VAR}").

    Thanks to StackOverflow:
    https://stackoverflow.com/questions/30734967/how-to-expand-environment-variables-in-python-as-bash-does

    :param path: Path.
    :param var_dict: Dictionary of replacement values. Defaults to `os.environ`

    :return: Path with environment variables substituted.
    """

    if var_dict is None:
        var_dict = os.environ

    def replace_var(m):
        var = m.group(2) if m.group(2) is not None else m.group(1)

        if var not in var_dict:
            raise RuntimeError('Missing environment variable "{}": {}'.format(var, path))

        return var_dict.get(var)

    return re.sub(r'(?<!\\)\$(\w+|\{([^}]*)\})', replace_var, path)


def get_cuda_device(config=None):
    """
    Get an available CUDA device.

    Available CUDA devices are a comma-separated string of device indexes (e.g. "0,1" for a 2 GPU system). The device
    list is supplied in config parameter "cuda_device" (e.g. "--config cuda_device=0") or the environment variable
    "CUDA_VISIBLE_DEVICES" (config takes precedence). The listed devices are checked for GPU/memory utilization and
    removed from the list if both are not 0%. This function returns the first free CUDA device from the list of
    available devices.

    :param config: Config. May contain "cuda_device" config parameter. If `None`, config is not checked for available
        devices (only CUDA_VISIBLE_DEVICES environment variable).

    :return: First available and free CUDA device.
    """

    if config is not None and 'cuda_device' in config:
        cuda_device = config['cuda_device']
    else:
        cuda_device = os.environ.get('CUDA_VISIBLE_DEVICES', None)

    if cuda_device is None:
        raise RuntimeError('No CUDA device in config ("--config cuda_device=X") or CUDA_VISIBLE_DEVICES')

    cuda_device_list = [int(val) for val in cuda_device.split(',')]

    cuda_device_free = free_gpu_list()

    cuda_device_list = [val for val in cuda_device_list if val in cuda_device_free]

    if len(cuda_device_list) == 0:
        raise RuntimeError(f'No available or configured CUDA devices were free: CUDA devices = {cuda_device}')

    return cuda_device_list[0]


def free_gpu_list():
    """
    Get a list of GPU IDs for free GPUs (no GPU or memory utilization).

    :return: A list of free GPUs.
    """

    # Help from https://discuss.pytorch.org/t/it-there-anyway-to-let-program-select-free-gpu-automatically/17560
    gpu_stats = subprocess.check_output(
        ['nvidia-smi', '--format=csv', '--query-gpu=index,utilization.gpu,utilization.memory']
    )

    df = pd.read_csv(
        io.StringIO(gpu_stats.decode()),
	    names=['INDEX', 'GPU_USED', 'MEM_USED'],
	    skiprows=1
    )

    df = df.loc[df['GPU_USED'].apply(lambda val: val.strip().split(' ')[0]) == '0']
    df = df.loc[df['MEM_USED'].apply(lambda val: val.strip().split(' ')[0]) == '0']

    if df.shape[0] > 0:
        return list(df['INDEX'].astype(int))

    return list()
