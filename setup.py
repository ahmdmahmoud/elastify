from setuptools import setup
import os

reqfile = os.path.join(os.path.dirname(__file__), 'requirements.txt')
with open(reqfile, 'r') as f:
      requirements = [line.strip() for line in f.readlines()]

setup(name='elastify',
      version='0.1',
      description='Client library for elasticsearch',
      author="anonymous",
      author_email="anonymous",
      packages=['elastify', 'elastify.l2r_features'],
      install_requires=requirements,
      entry_points={
            'console_scripts': [
                  'cutset=elastify.cutset:main',
                  'extractor=elastify.extractor:main',
                  'indices=elastify.indices:main',
                  'elastify=elastify.elastify:main',
                  'querify=elastify.querify:main',
                  'trainify=elastify.trainify:main',
                  'log2query=elastify.log2query:main', 
			'querify4labeled=elastify.querify4labeled:main'
                  'log2query=elastify.log2query:main',
                  'gstrainify=elastify.trainify_gs:main',
                  'nogstrainify=elastify.trainify_nogs:main',
            ]
      }
      )

