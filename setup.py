from setuptools import setup, find_packages 
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'DESCRIPTION.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='rabaDB',

    version='2.0',

    description="Store, Search, Modify your objects easily. You're welcome.",
    long_description=long_description,
    
    url='https://github.com/tariqdaouda/rabaDB',

    author='Tariq Daouda',
    author_email='tariq.daouda@umontreal.ca',

    license='ApacheV2.0',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 5 - Production/Stable',

        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Software Development :: Libraries',
        'Topic :: Database',
		'Topic :: Database :: Database Engines/Servers',
		'Topic :: Database :: Front-Ends',

        'License :: OSI Approved :: Apache Software License',

        'Programming Language :: Python :: 3.7',
    ],

    keywords='NoSQL database ORM sqlite3',

    packages=find_packages(exclude=['trash']),

    # List run-time dependencies here.  These will be installed by pip when your
    # project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/technical.html#install-requires-vs-requirements-files
    #~ install_requires=[],

    # If there are data files included in your packages that need to be
    # installed, specify them here.  OBSOLETE [If using Python 2.6 or less, then these
    # have to be included in MANIFEST.in as well.]
    #~ package_data={
        #~ 'sample': ['package_data.dat'],
    #~ },

    # Although 'package_data' is the preferred approach, in some case you may
    # need to place data files outside of your packages.
    # see http://docs.python.org/3.4/distutils/setupscript.html#installing-additional-files
    # In this case, 'data_file' will be installed into '<sys.prefix>/my_data'
    #~ data_files=[('my_data', ['data/data_file'])],

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
        'console_scripts': [
            'sample=sample:main',
        ],
    },
)
