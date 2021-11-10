class CloudProvider(object):

    def __init__(self, *args, **kwargs):
        raise NotImplementedError('Constructor not overriden from base class')

    def upload_public_file(self, local_path, dest_path):
        raise NotImplementedError('Function not implemented in child class')
