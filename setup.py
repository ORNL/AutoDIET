from codecs import open
import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'autoDIET/__version__.py')) as f:
    __version__ = f.read().split("'")[1]

requirements = [
    'datafed',
    'h5py',
    'numpy',
    'tarfile',

    'tika',
    'wget',

    'imghdr',
    'pillow',

    'dropbox',
    'google-api-python-client',
    'google-auth-httplib2',
    'google-auth-oauthlib',
    ]

setup(
    name='autoDIET',
    version=__version__,
    description='Tools for automated ingestion of data into DataFed',
    # long_description=long_description,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Cython',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Scientific/Engineering :: Information Analysis'],
    keywords=['data', 'ingestion', 'management', 'upload', 'crawl',
              'thumbnail', 'metadata', 'science'],
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*",
                                    "tests"]),
    url='https://github.com/ORNL/AutoDIET',
    license='MIT',
    author='Suhas Somnath',
    author_email='somnaths@ornl.gov',
    install_requires=requirements,
    setup_requires=['pytest-runner'],
    tests_require=['unittest2;python_version<"3.0"', 'pytest'],
    platforms=['Linux', 'Mac OSX', 'Windows 10/8.1/8/7'],
    # test_suite='pytest',
    # dependency='',
    # dependency_links=[''],
    # include_package_data=True,
    # https://setuptools.readthedocs.io/en/latest/setuptools.html#declaring-dependencies
    extras_require={
    },
)
