import numpy as np

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
    )
