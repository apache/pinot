from setuptools import setup

setup(
  name='pinotui',
  packages=['pinotui'],
  version='0.1',
  description='Importable flask blueprint for Pinot web UI',
  author='Joe Gillotti',
  author_email='jgillotti@linkedin.com',
  keywords=[],
  classifiers=[],

  install_requires=[
    'flake8==2.4.0',
    'Flask==0.10.1',
    'itsdangerous==0.24',
    'Jinja2',
    'kazoo==2.0',
    'MarkupSafe==0.23',
    'mccabe==0.3',
    'pep8==1.5.7',
    'pyflakes==0.8.1',
    'PyYAML==3.11',
    'requests',
    'Werkzeug==0.10.1',
    'addict==0.4.0',
  ],

  include_package_data=True
)
