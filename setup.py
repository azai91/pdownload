from setuptools import setup

setup(name='pdownload',
      version='0.3.0',
      description='Parallel file downloader',
      author='Ben Harris',
      author_email='benharris247@gmail.com',
      license='Apache',
      packages=['pdownload'],
      entry_points={
          'console_scripts': [
              'pdownload = pdownload.pdownload:main'
          ]
      },
      zip_safe=False,
      install_requires=[
          'requests',
          'tqdm'
      ])
