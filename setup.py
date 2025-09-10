from setuptools import setup, find_packages

setup(
    name='data_pull',
    version='0.1.0',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'data-pull=data_pull.data_pull:main',
        ],
    },
    description='A simple example package',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/your_package',
    author='Your Name',
    author_email='your.email@example.com',
    license='MIT',
    install_requires=[
        'pandas>=1.0.0',
        'blpapi',
        'requests>=2.25.1',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)