"""
Setup script for the Heartbeat Monitoring Pipeline package.
"""

from setuptools import find_packages, setup

setup(
    name="heartbeat-monitoring-pipeline",
    version="1.0.0",
    author="Emmanuel Kabu",
    description="Real-Time Customer Heartbeat Monitoring System",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "confluent-kafka>=2.3.0",
        "psycopg2-binary>=2.9.9",
        "python-dotenv>=1.0.0",
        "pandas>=2.1.0",
    ],
    extras_require={
        "dashboard": ["streamlit>=1.29.0"],
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.12.0",
            "isort>=5.13.0",
            "flake8>=7.0.0",
            "mypy>=1.8.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "heartbeat-pipeline=src.main:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Medical Science Apps.",
    ],
)
