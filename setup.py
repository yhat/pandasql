from distutils.core import setup
from setuptools import find_packages

with open("requirements.txt", "r") as f:
    requirements = f.read().splitlines()

setup(
    name="pandasql",
    version="0.7.3",
    author="Greg Lamp",
    author_email="greg@yhathq.com",
    url="https://github.com/yhat/pandasql/",
    license="MIT",
    packages=find_packages(),
    package_dir={"pandasql": "pandasql"},
    package_data={"pandasql": ["data/*.csv"]},
    description="sqldf for pandas",
    long_description=open("README.rst").read(),
    install_requires=requirements,
    classifiers=[
        "License :: OSI Approved :: MIT License",
    ],
)
