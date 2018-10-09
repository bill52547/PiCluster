from setuptools import setup, find_packages
setup(name='dxl-cluster',
      version='0.0.5',
      description='Cluster utility library.',
      url='https://github.com/tech-pi/dxcluster',
      author='Hong Xiang',
      author_email='hx.hongxiang@gmail.com',
      license='MIT',
      namespace_packages=['dxl'],
      packages=find_packages('src/python'),
      package_dir={'': 'src/python'},
      install_requires=[
          'click',
          'rx',
      ],
      scripts=[],
      zip_safe=False)
