from distutils.core import setup

from setuptools import find_packages

setup(
    name='equity_db',
    version='1.2',
    packages=find_packages(),
    package_data={'': ['variables/metadata/*.csv']},
    license='MIT',
    description='Python package which manages CRSP & Compustat data. Uses MongoDB to store and query data.',
    author='Alex DiCarlo',
    author_email='dicarlo.a@northeastern.edu',
    url='https://github.com/Alexd14/equity-db',
    download_url='https://github.com/Alexd14/equity-db/archive/refs/tags/v1.1.tar.gz',
    keywords=['CRSP', 'Compustat', 'stock database'],
    install_requires=[
        'pandas',
        'numpy',
        'pymongo',
        'pandas_market_calendars',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
