import os
import imghdr
from PIL import Image
from .parser import Parser


class Images(Parser):

    def __init__(self, file_path, scratch=None, verbose=False):
        """
        Parser capable of generating a thumbnail of the provided image.
        Metadata is extracted by default using Apache Kita

        Parameters
        ----------
        file_path : str
            Path to a single image file
        scratch : str, optional.
            path to directory that can be used for scratch purposes such as
            storing thumbnails or other temporary needs
        verbose : bool, optional
            Set to True to print statements for debugging purposes. Leave False
            otherwise. Default = False
        """
        super(Images, self).__init__(file_path, scratch=scratch,
                                     verbose=verbose)
        if not imghdr.what(self.file_path):
            raise TypeError("Unable to read file: " + self.file_path)

    def get_thumbnails(self, base_name, max_size=256):
        """
        Generates a thumbnail of the provided image

        Parameters
        ----------
        base_name : str
            Prefix for the thumbnail image file. Use DataFed record ID here.
        max_size : int, optional
            Size of the largest dimension in the image

        Returns
        -------
        list:
            List of size 1 whose content is a tuple of size 2 which is as:
            "Image", Path to a thumbnail image file
        """
        try:
            image = Image.open(self.file_path)
            if self.verbose:
                print('Opened image: {}, of size: {}'
                      ''.format(self.file_path, image.size))
            scal = max_size / max(image.size)
            new_size = (int(image.size[0] * scal), int(image.size[1] * scal))
            if self.verbose:
                print('Generating smaller image of shape: {}'.format(new_size))

            image.thumbnail(new_size)

            folder, large_file_name = os.path.split(self.file_path)
            ext = large_file_name.split('.')[-1]

            # Browsers don't support TIFF so export to PNG instead
            # TODO: Export to PNG when original extension is TIFF
            # if ext.lower() in ['tif', 'tiff']:

            small_file_name = base_name + '.' + ext

            if self.scratch:
                folder = self.scratch
            small_file_path = os.path.join(folder, small_file_name)

            if self.verbose:
                print('Writing thumbnail to: ' + small_file_path)

            image.save(small_file_path)

            return [("Image", small_file_path)]
        except IOError:
            return None
