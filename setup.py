from setuptools import setup
import sys

setup(
    name='giganews',
    version='0.0.2',
    author='Jacob M. Johnson',
    author_email='jake@archive.org',
    packages=['giganews'],
    url='https://github.com/jjjake/giganews',
    license='LICENSE',
    description='A python interface to giganews',
    long_description=open('README.md').read(),
    install_requires=[
        'pynntp==0.8.4',
        'futures==2.1.6',
        'gevent==23.9.1',
        'chardet==2.2.1',
        'python-magic==0.4.6',
        'internetarchive==0.5.7',
    ],
)
