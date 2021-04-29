
from setuptools import setup

__version__ = '0.1.0'


with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='submitter',
    version=__version__,
    author='Mike Richman',
    author_email='mike.d.richman@gmail.com',
    packages = ['submitter'],
    description='Job submission helper developed at UMD',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='http://code.icecube.wisc.edu/svn/sandbox/richman/submitter',
    install_requires=['numpy'],
)
