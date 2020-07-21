#  Copyright (c) 2020 Seven Bridges. See LICENSE

import pathlib
from datetime import datetime
from setuptools import setup, find_packages

current_path = pathlib.Path(__file__).parent

name = "sbpack"
version = open("sbpack/version.py").read().split("=")[1].strip().strip("\"")
now = datetime.utcnow()
desc_path = pathlib.Path(current_path, "Readme.md")
long_description = desc_path.open("r").read()

setup(
    name=name,
    version=version,
    packages=find_packages(),
    platforms=['POSIX', 'MacOS', 'Windows'],
    python_requires='>=3.6',
    install_requires=[
        "ruamel.yaml >= 0.15.77",
        "sevenbridges-python >= 0.20.2",
        "cwlformat"
    ],
    entry_points={
        'console_scripts': [
            'sbpack = sbpack.pack:main',
            'cwlpack = sbpack.pack:localpack',
            'sbpull = sbpack.unpack:main'
        ],
    },

    author='Seven Bridges',
    maintainer='Seven Bridges',
    maintainer_email='kaushik.ghose@sbgenomics.com',
    author_email='kaushik.ghose@sbgenomics.com',
    description='Command line tool to upload and download CWL to and from SB powered platforms.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
    license='Copyright (c) {} Seven Bridges'.format(now.year),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3 :: Only'
    ],
    keywords='seven bridges cwl common workflow language'
)
