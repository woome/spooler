import sys
from setuptools import setup

requires=['configobj']
if sys.version_info < (2, 6):
    requires.append('multiprocessing')

setup(
    name='spooler',
    version='0.1',
    author='Woome',
    author_email='tech@woome.com',
    packages=['sigasync'],
    install_requires=requires,
)

