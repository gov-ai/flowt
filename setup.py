# USAGE:
# build sharable file: python setup.py sdist
# install sharable file: pipenv install path/or/link/to/flowt-x.x.x.tar.gz

from distutils.core import setup

setup(
    name='flowt',
    version='0.0.0.0',
    packages=['flowt', ],
    license='Apache License, Version 2.0',
)
