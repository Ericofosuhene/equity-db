from distutils.core import setup

setup(
    name='equity_db',
    packages=['equity_db'],
    version='0.2',
    license='MIT',
    description='Python package which manages CRSP & Compustat data. Uses MongoDB to store and query data.',
    author='Alex DiCarlo',
    author_email='dicarlo.a@northeastern.edu',
    url='https://github.com/Alexd14/equity-db',
    download_url='https://github.com/Alexd14/equity-db/archive/refs/tags/v0.2.tar.gz',
    keywords=['CRSP', 'Compustat', 'stock database'],
    install_requires=[
        'pandas',
        'numpy',
        'pymongo',
        'pandas_market_calendars',
        'tqdm',
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
