from setuptools import setup, find_packages
import finance_tools

setup(
    name='finance_tools',
    version=finance_tools.__version__,
    packages=find_packages(),
    install_requires=['requests', 'pandas'],
)