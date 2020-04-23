import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='vsc-tools-lib-gjbex',  
    version='0.9',
    author="Geert Jan Bex",
    author_email="geertjan.bex@uhasselt.be",
    description="Python library for interacting with PBS, Adaptive Moab and MAM",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gjbex/vsc-tools-lib",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
)
