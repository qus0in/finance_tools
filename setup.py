from setuptools import setup, find_packages

setup(
    name='finance_tools',
    version='0.0.1',
    packages=find_packages(),
    install_requires=['requests', 'pandas'],
)