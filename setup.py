# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='reconciler',
    version='0.0.1',
    description='Reconcile S3 and Redshift',
    author='Rob Story',
    author_email='wrobstory@gmail.com',
    license='MIT License',
    url='https://github.com/wrobstory/reconciler',
    classifiers=['Development Status :: 4 - Alpha',
                 'Programming Language :: Python',
                 'Programming Language :: Python :: 2',
                 'License :: OSI Approved :: MIT License'],
    install_requires=['psycopg2==2.5.4', 'boto==2.33.0']
)
