# Copyright 2023 Mitchell Eccles.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License


import sys
import os

from setuptools import find_packages, setup


setup(
    name='airflow_dbt_common',
    version='1.0.1',
    description='A collection of functions that are needed to run DBT cosmos jobs on airflow',
    long_description='A collection of functions that are needed to run DBT cosmos jobs on airflow',
    long_description_content_type='text/markdown',
    author='Mitch Eccles <me@mitch.uk>',
    url='https://github.com/HedinghamAI/airflow_dbt_common',
    packages=[
        package for package in find_packages(exclude=('tests', 'docs'))
        if package.startswith('airflow_dbt_common')
    ],
    license='MIT',
    python_requires='>=3.6',
    install_requires=[
      'astronomer-cosmos==1.2.5',
      'astro-sdk-python==1.7.0'
    ],

    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent'
    ],
    zip_safe=False
)