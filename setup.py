from setuptools import setup, find_packages
 
try:
    # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:
    # for pip <= 9.0.3
    from pip.req import parse_requirements

def load_requirements(fname):
    reqs = parse_requirements(fname)
    return [str(ir.req) for ir in reqs]


setup(name='DataStoryPatternLibrary',
      version='0.1',
      url='https://github.com/MaciejJanowski/DataStoryPatternLibrary',
      license='MIT',
      author='Maciej Janowski',
      author_email='maciej.janowski@insight-centre.org',
      description='Data Story Pattern Analysis for LOSD',
      packages=find_packages(exclude=['tests']),
      install_requires=load_requirements("requirements.txt"),
      long_description=open('README.md').read(),
      zip_safe=False)