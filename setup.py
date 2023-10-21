import setuptools

with open('README.md', 'r') as f:
    long_description = f.read()

setuptools.setup(
    name='pg-streamline',
    version='1.1.0',
    author='Shwetabh Kumar',
    author_email='shwetabh002@gmail.com',
    description='pg-streamline is a Python library designed to simplify and streamline the process of logical replication',
    license='MIT',
    long_description=long_description,
    long_description_content_type='text/markdown',
    keywords=['netsuite', 'api', 'python', 'sdk'],
    url='https://github.com/shwetabhk/pg-streamline-py',
    packages=setuptools.find_packages(),
    install_requires=['pika>=1.3.2', 'psycopg2-binary>=2.9.9', 'pyyaml>=6.0.1'],
    classifiers=[
        'Topic :: Internet :: WWW/HTTP',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ]
)