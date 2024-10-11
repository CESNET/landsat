import numpy as np
from pathlib import Path

from skimage import exposure


def normalize(array):
    array_min, array_max = array.min(), array.max()
    return (array - array_min) / (array_max - array_min)


def linear_stretch(array, p_low=2, p_high=98):
    p2, p98 = np.percentile(array, (p_low, p_high))
    return exposure.rescale_intensity(array, in_range=(p2, p98))


def gamma_correction(image, gamma=1.0):
    return np.power(image, gamma)


def replace_tif_to_jpg(filename):
    return (
        filename
        .replace('.tif', '.jpg')
        .replace('.TIF', '.jpg')
        .replace('.tiff', '.jpg')
        .replace('.TIFF', '.jpg')
        .replace('.jpeg', '.jpg')
    )


def rename(old_file_path: Path, new_file_path: Path):
    if old_file_path.suffix != new_file_path.suffix:
        if not (
                (old_file_path.suffix.lower() in {'.jpg', '.jpeg'})
                and
                (new_file_path.suffix.lower() in {'.jpg', '.jpeg'})
        ):
            from exceptions.thumbnail_generation import RenameFileExtensionsDoNotMatch
            raise RenameFileExtensionsDoNotMatch(old_file_path.name, new_file_path.name)

    if new_file_path.exists():
        from exceptions.thumbnail_generation import RenameFileExistsError
        raise RenameFileExistsError(new_file_path)

    old_file_path.rename(new_file_path)
    return old_file_path
