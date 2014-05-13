import os
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))
requires = open(os.path.join(here, 'requirements.txt')).read()
readme = open(os.path.join(here, 'README.md')).read()

setup(
    name='dataplunger',
    version='0.1.0',
    author='Matthew Kenny',
    author_email='matthewkenny AT gmail DOT com',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Topic :: Scientific/Engineering :: GIS',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Intended Audience :: Developers'
    ],
    packages=['dataplunger'],
    url='https://github.com/mattmakesmaps/dataplunger',
    license='GNU GENERAL PUBLIC LICENSE V2',
    keywords='gis etl',
    description='Extract, Transform, Load',
    long_description=readme,
    install_requires=requires,
    test_suite='nose.collector',
    tests_require=['nose', 'nose-cover3'],
    include_package_data=True
)
