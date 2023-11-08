from setuptools import setup, find_packages

setup(
    name='Coding Assessment',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pyspark'
    ],
    py_modules=[
        'kommatiparadataprepapp'
    ],
    entry_points={
        'console_scripts': [
            '[script name]=[module name]:[function name]'
        ]
    }
)
