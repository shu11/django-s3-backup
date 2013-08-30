# -*- coding: utf-8 -*-
from django.core.management.base import BaseCommand
from django.db import DEFAULT_DB_ALIAS
from optparse import make_option
from django_s3_backup.utils import Restore


myusg = """\
./manage.py restore.py --database=[database_name]
<description>
 This program restore into entire Database data from S3, including all table.
 The data is restored from the S3, for which the account information is used
 by AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the OS enviroments.
"""

class Command(BaseCommand):
    help = myusg
    option_list = BaseCommand.option_list\
                  + ( make_option("--database", action="store", dest="db", default=None
                                  , help=u"database name."),
                      make_option("--excludes", action="store", dest="excludes", default=None
                                  , help=u"ignore list of table name."),
                      )

    def handle(self, *args, **options):
        '''
        dbname = options["db"]
        excludes = options["excludes"]
        if not dbname:
            print "[Error] database name is required..."
            print myusg
            exit(1)
        '''
        print "Starting restore..."
        #r = Restore(using="default", use_transaction=False)
        r = Restore(using="default")
        r.flush_all_tables()
        r.s3_to_database()
        print "End restore process successfully!"
