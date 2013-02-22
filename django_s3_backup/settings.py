# -*- coding: utf-8 -*-
#from django.conf import settings
from django.conf import settings

PROJECT_ROOT = getattr(settings, "PROJECT_ROOT", None)
INSTALLED_APPS = getattr(settings, "INSTALLED_APPS", None)
MULTI_UPLOAD_THREASHOLD_SIZE = 5 * 1024 * 1024          # 5MB以上でないとマルチパート転送してはいけない。
EXPIRE_DAY = 2
HTTPS = True    # HTTPS通信。FalseにするとHTTP通信でS3にアップする。
BASE_DIR = "database"
try:
    # 既に設定されている場合
    value = settings.S3_END_POINT
except AttributeError, e:
    # 設定されていない場合はここで設定する。
    """
    【S3エンドポイント一覧】
    US Standard *                       s3.amazonaws.com                (none required)
    US West (Oregon) Regions            s3-us-west-2.amazonaws.com      us-west-2
    US West (Northern California)       s3-us-west-1.amazonaws.com      us-west-1
    EU (Ireland) Region                 s3-eu-west-1.amazonaws.com      EU
    Asia Pacific (Singapore) Regions    3-ap-southeast-1.amazonaws.com  ap-southeast-1
    Asia Pacific (Sydney) Region        s3-ap-southeast-2.amazonaws.com ap-southeast-2
    Asia Pacific (Tokyo) Region         s3-ap-northeast-1.amazonaws.com ap-northeast-1
    South America (Sao Paulo) Region    s3-sa-east-1.amazonaws.com      sa-east-1
    """
    settings.S3_END_POINT = "s3-ap-northeast-1.amazonaws.com"
