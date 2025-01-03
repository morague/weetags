from pathlib import Path
from setuptools import find_packages, setup


version = (Path(__file__).parent / "weetags/VERSION").read_text("ascii").strip()

install_requires = [
    "attrs>=23.2.0",
    "pyyaml>=6.0.0"
]

extras_require = {
    ':platform_python_implementation == "CPython"': ["PyDispatcher>=2.0.5"],
    ':platform_python_implementation == "PyPy"': ["PyPyDispatcher>=2.1.0"],
}

setup(
    name="weetags",
    version=version,
    project_urls={
        "Source": "https://github.com/morague/weetags",
        "Issues": "https://github.com/morague/weetags/issues"
    },
    author= "Romain Viry",
    author_email= "rom88.viry@gmail.com",
    maintainer= "Romain Viry",
    maintainer_email= "rom88.viry@gmail.com",
    description = "Small & simple Persistent tree library",
    python_requires=">=3.11",
    install_requires=install_requires,
    extras_require=extras_require,
    packages=find_packages(
        where=".", 
        exclude=(
            "tests",
            "tests.*",
            "weetags.app",
            "weetags.app.*",
        )
    ),
    package_dir={"weetags": "weetags"},
    package_data={"weetags": ["*"]},
    include_package_data=True,
    zip_safe=False,
)