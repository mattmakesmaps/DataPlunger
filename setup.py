import os
from setuptools import setup
from distutils.extension import Extension
from Cython.Build import cythonize

here = os.path.abspath(os.path.dirname(__file__))
requires = open(os.path.join(here, 'requirements.txt')).read()
readme = open(os.path.join(here, 'README.md')).read()
extensions = [Extension("dataplunger.c_processorchangecase", ["dataplunger/c_processorchangecase.pyx"])]

setup(
    name='dataplunger',
    version='0.1.0',
    author='Matthew Kenny',
    author_email='matthewkenny AT gmail DOT com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Topic :: Scientific/Engineering :: GIS',
        'License :: OSI Approved :: GNU General Public License v2 (GPLv2)',
        'Intended Audience :: Developers'
    ],
    packages=['dataplunger'],
    ext_modules = cythonize(extensions),
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