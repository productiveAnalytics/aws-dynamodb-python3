# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENCE') as f:
    license = f.read()

setup(
    name='aws-dynamodb-python3-scan',
    version='0.0.1',
    description='Utility to clean PayerInfo table',
    long_description=readme,
    author='Lalit Chawathe',
    author_email='me@productiveAnalytics.com',
    url='https://github.com/kennethreitz/samplemod',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)