from setuptools import setup, find_packages

setup(name='dxl-cluster',
      version='0.0.6',
      description='Cluster utility library.',
      url='https://github.com/tech-pi/dxcluster',
      author='Hong Xiang',
      author_email='hx.hongxiang@gmail.com',
      license='MIT',
      namespace_packages=['dxl'],
      packages=find_packages('src/python'),
      package_dir={'': 'src/python'},
      install_requires=[
          'jfs==0.1.3',
          'sqlalchemy',
          'flask-restful',
          'typing',
          'marshmallow',
          'networkx',
          'apscheduler',
          'arrow',
          'fs',
          'click',
          'rx',
          'pyyaml',
          'attrs',
          'marshmallow>=3.0.0b16',
          'psycopg2',
          'flask',
          'flask-restful',
          'arrow',
          'requests',
          'sqlalchemy'
      ],
      entry_points='''
            [console_scripts]
            dxcluster=dxl.cluster.cli:cli
      ''',
      scripts=[],
      zip_safe=False)
