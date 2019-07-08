from setuptools import setup, find_packages
 




setup(name='DataStoryPatternLibrary',
      version='0.1.4.1',
      url='https://github.com/MaciejJanowski/DataStoryPatternLibrary',
      license='MIT',
      author='Maciej Janowski',
      author_email='maciej.janowski@insight-centre.org',
      description='Data Story Pattern Analysis for LOSD',
      packages=find_packages(exclude=['tests']),
      install_requires=['SPARQLWrapper',
                        'sparql_dataframe','pandas',
                        'numpy','scipy'],
      long_description=open('README.md').read(),
      zip_safe=False)