from setuptools import setup, find_packages
 




setup(name='datastories',
      version='0.3.10',
      url='https://github.com/MaciejJanowski/DataStoryPatternLibrary',
      license='MIT',
      author='Maciej Janowski',
      author_email='maciej.janowski@insight-centre.org',
      description='Data Story Pattern Analysis for LOSD',
      packages=find_packages(exclude=['tests']),
      install_requires=['SPARQLWrapper==1.8.4',
                        'pandas==0.24.2',
                        'sparql-dataframe==0.3',
                        'numpy==1.16.2',
                        'scipy==1.2.1'],
      long_description=open('README.md').read(),
      long_description_content_type='text/markdown',
      zip_safe=False)
