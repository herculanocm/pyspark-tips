import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="de_pyspark_tips",
    version="0.0.2",
    author="Herculano Cunha",
    author_email="herculanocm@outlook.com",
    description="Data Engineer tips for pyspark",
    download_url='https://github.com/herculanocm/pyspark-tips/archive/master.zip',
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
     keywords='AWS Glue Glue3 tools tips',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
    install_requires=['pyspark>=3.1,<3.3'],
)