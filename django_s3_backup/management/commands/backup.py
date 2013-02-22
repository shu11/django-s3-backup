# -*- coding: utf-8 -*-
from django.core.management.base import BaseCommand
from optparse import make_option
from django.conf import settings
from datetime import datetime
from django_s3_backup.utils import BackUp


myusg = """\
./manage.py backup
<description>
 This program backup entire Database data, including all table.
 The data is upload to the S3, for which the account information is used
 by AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the OS enviroments.
"""

class Command(BaseCommand):
    help = myusg
    '''
    option_list = BaseCommand.option_list\
                  + ( make_option("--input", action="store", dest="ifn", default=None
                                  , help=u"Input Patent CSV File."),
                      make_option("--lineno", action="store", dest="lineno", default=0
                                  , help=u"Start line number [0:x-1], where x is number of lines for the csv"),
                      )
    '''
    def handle(self, *args, **options):
        print "Starting backup..."
        b = BackUp(using="default")
        b.update_models()
        print "End backup process successfully!"
