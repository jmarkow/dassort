from setuptools import setup

setup(
    name='dassort',
    author='Jeff Markowitz',
    description='Donut forget the dassort',
    version='0.01a',
    platforms=['mac','unix'],
    install_requires=['Click', 'pyyaml', 'hashlib'],
    python_requires='>=3',
    entry_points={'console_scripts':['dassort = dassort:dassort']}
)
