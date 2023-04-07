import setuptools
import re

_version_re = re.compile(r"__version__\s+=\s+(.*)")

with open("flink-sql-gateway-client") as f:
    version = _version_re.search(f.read().decode("utf-8"))
    assert version is not None
    version = version.group(1)

all_require = []
tests_require = all_require + []

setuptools.setup(
    name="py_flink_sql_gateway_client",
    version=version,
    author="dormi330",
    author_email="dormi330@gmail.com",
    url="https://github.com/zqWu/flink-sql-gateway-client",
    description="python client for flink sql gateway",
    long_description="""
    client for Flink sql gateway (https://flink.apache.org/)
    provides a DBAPI 2.0 implementation
    """,
    license="Apache 2.0",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: Apache License",
    ],
    python_requires=">=3.7",
    install_requires=["requests"],
)
