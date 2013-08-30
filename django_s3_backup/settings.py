# -*- coding: utf-8 -*-
#from django.conf import settings
from django.conf import settings

PROJECT_ROOT = getattr(settings, "PROJECT_ROOT", "")
INSTALLED_APPS = getattr(settings, "INSTALLED_APPS", [])
MULTI_UPLOAD_THREASHOLD_SIZE = 5 * 1024 * 1024          # 5MB以上でないとマルチパート転送してはいけない。

# 設定されていない場合はここで設定する。
#    【S3エンドポイント一覧】
#    US Standard *                       s3.amazonaws.com                (none required)
#    US West (Oregon) Regions            s3-us-west-2.amazonaws.com      us-west-2
#    US West (Northern California)       s3-us-west-1.amazonaws.com      us-west-1
#    EU (Ireland) Region                 s3-eu-west-1.amazonaws.com      EU
#    Asia Pacific (Singapore) Regions    3-ap-southeast-1.amazonaws.com  ap-southeast-1
#    Asia Pacific (Sydney) Region        s3-ap-southeast-2.amazonaws.com ap-southeast-2
#    Asia Pacific (Tokyo) Region         s3-ap-northeast-1.amazonaws.com ap-northeast-1
#    South America (Sao Paulo) Region    s3-sa-east-1.amazonaws.com      sa-east-1

BUCKET = getattr(settings, 'S3_BACKUP_BUCKET', None)
ACCESS_KEY = getattr(settings, 'S3_BACKUP_ACCESS_KEY', None)
SECRET_KEY = getattr(settings, 'S3_BACKUP_SECRET_KEY', None)
DOMAIN = getattr(settings, 'S3_BACKUP_DOMAIN', 's3.amazonaws.com')
IS_SECURE = getattr(settings, 'S3_BACKUP_USE_SSL', True)
EXPIRE_DAYS = getattr(settings, 'S3_EXPIRE_DAYS', 7)
DIRECTORY = getattr(settings, 'S3_BACKUP_DIRECTORY', "django-database/")
DIRECTORY = '%s/' % DIRECTORY.strip('/')

