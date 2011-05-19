import sys
from setuptools import setup

requires=['configobj']
if sys.version_info < (2, 6):
    requires.append('multiprocessing')


setup(
    name='spooler',
    version='0.1',
    description='Simple task queue using the filesystem',
    long_description="""Task queue that uses a filesystem backend. It comes with an interface to Django signals, and can be easily extended.""",
    author='Woome',
    author_email='patrick@woome.com',
    license = "GNU GPL v3",
    url = "http://github.com/woome/spooler",
    download_url = "http://github.com/woome/spooler/tarball/master",
    packages=['sigasync'],
    install_requires=requires,
    platforms=['any'],
    keywords=['asynchronous', 'queue', 'task queue'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        ],
    package_data={'sigasync': ['configspec.ini']},
)

