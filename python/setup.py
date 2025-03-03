from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="featherstore-client",
    version="0.1.0",
    author="FeatherStore Team",
    author_email="info@featherstore.io",
    description="Python client for FeatherStore feature store",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TFMV/featherstore",
    project_urls={
        "Bug Tracker": "https://github.com/TFMV/featherstore/issues",
        "Documentation": "https://github.com/TFMV/featherstore/tree/main/python",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Database",
    ],
    packages=find_packages(),
    py_modules=["featherstore_client"],
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.0.0",
        "numpy>=1.18.0",
        "pyarrow>=5.0.0",
        "requests>=2.25.0",
    ],
)
