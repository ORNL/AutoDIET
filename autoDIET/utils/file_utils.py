import os
import tarfile
from warnings import warn


def make_tarfile(source_dir, tar_path=None, compress=True):
    """
    Compresses the provided directory to a tar file.

    Parameters
    ----------
    source_dir : str
        Path to a directory
    tar_path : str, Optional.
        Path for resulting tar file or directory to place the tar ball
    compress : bool, Optional. Default = True
        Whether or not to compress the provided folder using gzip

    Returns
    -------
    str : path to compressed file
    """
    make_tar_path = tar_path is None
    parent, tar_name = os.path.split(source_dir)

    if tar_path:
        if not isinstance(tar_path, str):
            warn('tar_path was not a valid string')
            tar_path = None
            # Same as if the tar_path was not specified
            make_tar_path = True
        else:
            if os.path.isdir(tar_path):
                # Only destination dir provided
                # Swap the parent, rest of the steps are the same
                parent = tar_path
                make_tar_path = True
            else:
                # Full path for tar ball provided
                make_tar_path = False

    if make_tar_path:
        parts = tar_name.split('.')
        if len(parts) > 1:
            tar_name = '.'.join(parts[:-1])
        tar_path = os.path.join(parent, tar_name + '.tar')
    mode = "w"
    if compress:
        mode = "w:gz"
    with tarfile.open(tar_path, mode) as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))
    return tar_path


def validate_scratch_dir(scratch, verbose=False):
    """
    Checks validity and write access to specified scratch directory

    Parameters
    ----------
    scratch : str, optional.
        path to directory that can be used for scratch purposes such as
        storing thumbnails or other temporary needs
    verbose : bool, optional
        Set to True to print statements for debugging purposes. Leave False
        otherwise. Default = False

    Returns
    -------
    str
        Path to scratch directory if valid. Else, None.
    """
    if scratch:
        if not os.path.exists(scratch):
            if verbose:
                print('Provided scratch does not exist: {}'.format(scratch))
            scratch = None
        elif not os.path.isdir(scratch):
            if verbose:
                print('Provided scratch is not a dir: {}'.format(scratch))
            scratch = None
        elif not os.access(scratch, os.W_OK):
            if verbose:
                print('No write access to scratch dir: {}'.format(scratch))
            scratch = None
    return scratch


def move_file(orig_path, new_dir_path):
    """
    Moves the file or directory at the provided path to the provided directory
    path

    Parameters
    ----------
    orig_path : str
        Original path
    new_dir_path : str
        Path to the target directory

    Returns
    -------
    new_path : str
        New path of the provided file or directory
    """
    _, file_name = os.path.split(orig_path)
    new_path = os.path.join(new_dir_path, file_name)
    os.rename(orig_path, new_path)
    return new_path