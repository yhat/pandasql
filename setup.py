from distutils.core import setup
from setuptools import find_packages

setup(
    name="pandasql",
    version="0.0.1",
    author="Greg Lamp",
    author_email="lamp.greg@gmail.com",
    url="https://github.com/yhat/pandasql/",
    license=open("LICENSE.txt").read(),
    packages=find_packages(),
    description="sqldf for pandas",
    long_description=open("README.txt").read(),
)

