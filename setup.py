from setuptools import setup, find_packages

setup(name='utilsbigdata',
      version='0.6',
      description='Utils for Big Data - MTT',
      url='git@github.com:bigdata-sectra/utilsbigdata.git',
      author='Big Data - MTT',
      author_email='bigdata@sectra.gob.cl',
      license='MIT',
      packages=find_packages(),
      install_requires = [
            'numpy', 
            'pandas<0.25.0', 
            'requests'],
      zip_safe=False)