from setuptools import setup, find_packages

base_packages = ["pyspark>=3.0", "click"]
dev_packages = ["jupyter"]

setup(
    name="trending_topics",
    author="M&M",
    version="0.3",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=base_packages,
    extras_require={"dev": dev_packages},
    entry_points={"console_scripts": ["trending_topics = trending_topics.cli:main"]},
)
