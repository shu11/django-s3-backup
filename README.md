django-s3-backup
================

Backup all applications to Amazon S3  in INSTALLED_APPS of django project like loaddata/dumpdata.

Authors
----------------
- project started by Shuichi Mitarai (http://www.phactory.jp and CTO of http://lafla.co.jp)

Features
----------------
- backup and restore command for django-admin.py and manage.py. It's like a loaddata/dumpdata.
- Transfer backups to Amazon S3
- Memory Efficient stream JSON encoder/decoder are involved.
- Huge table can dump and restore.
- restore all dump data on S3 to local database transaction safely.

License
----------------
Distributed under the [MIT License][mit].
[MIT]: http://www.opensource.org/licenses/mit-license.php

Copyright &copy; 2013 Shuichi Mitarai.


Supported options
----------------
--exclude=[table_names]   exclude to backup or restore.
