from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
if __name__ == '__main__':
    setup(
        name="oc_preprocessing",
        version = "0.0.5",
        install_requires= ["setuptools>=61.0",
            "beautifulsoup4==4.12.2",
            "fakeredis==2.10.3",
            "ndjson==0.3.1",
            "oc_idmanager==0.2.6",
            "pandas==1.5.3",
            "requests==2.28.2",
            "tqdm==4.64.1",
            "zstandard==0.19.0"],
        author = "OpenCitations authors",
        author_email = "contact@opencitations.net",
        description=("This package is meant to preprocess OpenCitations source dumps so to make them easily usable in OpenCitations main processes, by deleting unused information, splitting big files, and validating identifiers"),
        license = "BSD",
        keywords = "preprocessing data dumps",
        url = "https://github.com/opencitations/preprocess",
        packages=["preprocessing", "preprocessing.finder", "preprocessing.identifier_manager", "preprocessing.datasource"],
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: ISC License (ISCL)",
            "Operating System :: OS Independent",
        ],
        include_package_data=True
    )