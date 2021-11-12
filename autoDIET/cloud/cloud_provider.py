class CloudProvider(object):
    """
    Abstract class for a cloud provider capable of uploading a file and hosting
    it such that it would be publicly available for embedding in web services
    """

    def __init__(self, *args, **kwargs):
        raise NotImplementedError('Constructor not overridden from base class')

    def upload_public_file(self, local_path, dest_path):
        raise NotImplementedError('Function not implemented in child class')
