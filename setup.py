from setuptools import setup, find_packages

setup(
    name='Amazon Distributed Runner',
    version='0.0',
    author='Bas Hoonhout',
    author_email='b.m.hoonhout@tudelft.nl',
    packages=find_packages(),
    description='A distributed scheduling system based on Amazon Web Services',
    install_requires=[
        'boto3',
        'fabric',
        'docopt',
        'configparser',
    ],
    #setup_requires=[
    #    'sphinx',
    #    'sphinx_rtd_theme'
    #],
    entry_points={'console_scripts': [
        'adr = adr.cmd:adr_cmd',
    ]},
)
