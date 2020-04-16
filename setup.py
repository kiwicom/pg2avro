import pathlib
from setuptools import setup, find_packages

# The text of the README file
HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text()

with open("requirements.in") as f:
    install_requires = [line.rstrip() for line in f if line and line[0] not in "#-"]

with open("requirements-test.in") as f:
    tests_require = [line.rstrip() for line in f if line and line[0] not in "#-"]

setup(
    name="pg2avro",
    version="0.2.3",
    license="MIT",
    description="Utility generating avro files from postgres.",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Milan Lukac",
    author_email="milan.lukac@kiwi.com",
    url="https://github.com/kiwicom/pg2avro",
    packages=find_packages(exclude=("tests",)),
    include_package_data=True,
    install_requires=install_requires,
    tests_require=tests_require,
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Environment :: Plugins",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
    ],
)
