
from setuptools import setup

__version__ = '0.2.0'


with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='submitter',
    version=__version__,
    author='Steve Sclafani, Mike Richman',
    author_email='mike.d.richman@gmail.com',
    packages = ['submitter'],
    description='Job submission helper developed at UMD',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='github.com/ssclafani949/Submitter',
    install_requires=['numpy'],
)
