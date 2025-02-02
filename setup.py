from setuptools import setup, find_packages

setup(
    name="solana_sentiment",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'sqlalchemy',
        'alembic',
        'psycopg2-binary',
    ],
)