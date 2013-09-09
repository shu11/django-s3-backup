import os
from distutils.core import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


packages = []
package_dir = "django_s3_backup"
for dirpath, dirnames, filenames in os.walk(package_dir):
    # ignore dirnames that start with '.'
    for i, dirname in enumerate(dirnames):
        if dirname.startswith("."):
            del dirnames[i]
    if "__init__.py" in filenames:
        pkg = dirpath.replace(os.path.sep, '.')
        if os.path.altsep:
            pkg = pkg.replace(os.path.altsep, '.')
        packages.append(pkg)


setup(
    name='django-s3-backup',
    version='0.8',
    description='Backup all applications to Amazon S3 in INSTALLED_APPS of django project like loaddata/dumpdata.',
    long_description=read('README.md'),
    author='Shuichi Mitarai',
    author_email='',
    install_requires=['boto'],
    license='MIT',
    url='https://github.com/lukas-hetzenecker/django-s3-backup',
    keywords=['django', 'database', 'backup', 'amazon', 's3'],
    packages=packages
)
