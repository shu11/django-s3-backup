# -*- coding: utf-8 -*-
#from django.core import serializers
from stream_serializer import Serializer as StreamSerializer
from stream_serializer import Deserializer as StreamDeserializer
from model_dependency import sort_relation_dependencies
from django.utils.datastructures import SortedDict
from django.db import transaction, models, router, DEFAULT_DB_ALIAS, connections
from django.core.management.sql import sql_flush
from django.core.management.base import CommandError
from django.core.management.commands import flush
from django.db.models import get_app, get_apps, get_model, get_models
from django.core.management.commands.dumpdata import sort_dependencies
from django.core.management.color import no_style
from django.utils.importlib import import_module
from boto.s3.connection import S3Connection, Location
from boto.s3.key import Key
from threading import Thread
from datetime import datetime, timedelta
import psycopg2
import os, sys, settings, time, random


class BackUp(object):
    _projname = ""
    _base_dir = settings.DIRECTORY
    _jsons = []
    _app_list = None
    _s3 = None
    def __init__(self, excludes=[], using=DEFAULT_DB_ALIAS):
        excluded_apps = set(get_app(app_label) for app_label in excludes)
        # excludesに指定されていないINSTALLED_APPSに登録されている全アプリオブジェクトを取得
        self._app_list = SortedDict((app, None) for app in get_apps() if app not in excludes)
        # S3オブジェクトを取得
        self._s3 = S3()
        # プロジェクト名を取得
        self._projname = self.get_project_name()
        self._uploaded_files = []
        self._start_date = datetime.now().strftime("%Y%m%d")
        self._using = using
        
    def get_project_name(self):
        project_root = settings.PROJECT_ROOT
        sPos = project_root.rfind("/")
        if ( sPos != -1 ):
            return project_root[sPos+1:]
        return None
                    
    def process_before(self):
        """
        前処理のためのI/F。バックアップストレージ保存処理前にコールされる。
        Trueを返せば、後続処理を実行。Falseはその場で処理終了。
        """
        return True

    def process_after(self):
        """
        後処理のためのI/F。バックアップストレージ保存処理後にコールされる。
        Trueを返せば、後続処理を実行。Falseはその場で処理終了。
        """
        try:
            # ここまで処理が来ていれば、古いものを削除
            days = settings.EXPIRE_DAYS
            self._s3.delete_old(days)
            return True
        except:
            return False
        
    def _update_models(self):
        if ( self.process_before() == False ):
            # 前処理
            return
        
        # INSTALLED_APPから全モデルクラスを取得
        self._using = DEFAULT_DB_ALIAS        # ToDo: defaultだけではなく、マルチDB化
        models = sort_dependencies(self._app_list.items())
        ofn = "/tmp/django_s3_backup_%s_" % ( datetime.now().strftime("%Y%m%d%H%M%S") )
        # print "Writing JSON to %s..." % ofn
        print "get all models Done."
        # appのmodel毎にjsonダンプ -> S3に保存
        stream_fp = None
        for model in models:
            # dbg start
            #if ( model.__name__ != "Customer" ):
            #    continue
            # dbg end
            if ( (not model._meta.proxy) and (router.allow_syncdb(self._using, model)) ):
                try:
                    # 全レコードをファイルofnへ書き出し
                    fsize = self.create_json_file(ofn, model, self._using)
                    if ( fsize == None ):
                        # レコード存在しない場合
                        continue
                    # ファイル内容をS3にアップロード
                    self.update_S3(model, ofn, fsize)
                except:
                    if ( stream_fp ):
                        stream_fp.close()
                        os.remove(ofn)
                    raise

        if ( self.process_after() == False ):
            # 後処理
            return
            
    def update_models(self):
        """
        excludesに指定されていないINSTALLED_APPSに登録されている全モデルをJSONダンプし、
        ストレージに保存。
        """
        try:
            self._update_models()
        except KeyboardInterrupt:
            if ( self._s3 ):
                self.delete_all()
            raise
            
    def create_json_file(self, ofn, model, using):
        """
        ofnに対してmodelで指定された全レコードをJSON形式で書き込む。
        書き込むべきレコードが１件も存在しない場合はNoneを返す。
        """
        print "\nReading <%s>: " % (model.__name__)
        sys.stdout.flush()
        allobjs = model._default_manager.using(using).all()
        #allobjs = model._default_manager.using(using).order_by("pk").all()
        objcnt = allobjs.count()
        if ( objcnt == 0 ):
            print " Skip."
            return None
        # stream_fp（ファイル）にjsonを格納
        print "   Starting writing all records[#%d]... " % objcnt
        stream_fp = open(ofn, "w")
        s = StreamSerializer()
        s.serialize(allobjs, stream=stream_fp, line_by_line=True)
        stream_fp.close()
        print "   File Output is Done\n"
        fsize = os.path.getsize(ofn)
        return fsize

    def delete_all(self):
        """
        アップロードしたテーブルを削除。
        ただし、当日分しか消さない。
        途中でアップロードが失敗した場合の掃除用メソッド。
        """
        for fkey in self._uploaded_files:
            self._s3.delete(fkey)
       
    def get_model_path(self, model):
        """
        modelインスタンスから対応するパスを取得して返す。
        ディレクトリパスだけで、ファイル名については各自対応すること。
        """
        clsname = "%s.%s" % ( model.__module__, model.__name__ )
        model_path = "%s/%s/%s/apps/%s" % ( self._base_dir, self._using, self._projname, clsname )
        return model_path

    def update_S3(self, model, json_fn, fsize):
        """
        modelの全レコード情報(json_fn)をS3にバックアップする。
        保存パス：
        /{{project_name}}/apps/{{model._meta.name}}/{{%Y%m%d.json}}
        """
        if not ( self._s3 ):
            return
        fn = "%s.json" % self._start_date
        path = "%s/%s" % ( self.get_model_path(model), fn )
        print "   <%s> to S3 [FSize: %d(byte)] ..." % ( path, fsize )
        result = self._s3.update_content(path, json_fn, fsize)
        self._uploaded_files.append(path)
        if ( result ):
            print "   Save Success!"
        else:
            print "   Save Failure!"


    def update_media(self, base_path=getattr(settings, "MEDIA_ROOT", "")):
        """
        settings.MEDIA_ROOTに指定されたパス配下をすべてS3にバックアップする。
        保存パス：
        /{{project_name}}/media/{{dir_name}}/{{fn}}
        """
        for fn in os.listdir(base_path):
            path = os.path.join(base_path, fn)
            if ( os.isdir(path) ):
                self.update_media(path)
            if not ( os.isfile(path) ):
                # シンボリックリンク等
                continue
            # ToDo: S3にセーブ
            return


class Uploader(Thread):
    """
    Parallel Multi Uploader
    """
    def __init__(self, mp, fn, no, spos, size):
        Thread.__init__(self)
        self._mp = mp
        self._fn = fn
        self._thread_no = no
        self._spos = spos
        self._size = size

    def run(self):
        """
        thread main.
        """
        fp = open(self._fn, "r")
        fp.seek(self._spos)
        if ( self._size ):
            self._mp.upload_part_from_file(fp, part_num=self._thread_no, size=self._size)
        else:
            self._mp.upload_part_from_file(fp, part_num=self._thread_no)
        fp.close()

        if ( self._size ):      end_pos = "%d" % ( self._spos + self._size )
        else:                   end_pos = "END"
        print "   Uploaded #%d (size: %d [%d:%s])..." % ( self._thread_no,
                                                          self._size if self._size else -1, self._spos, end_pos )
        

class S3(object):
    """
    環境変数（~/.zshrc、/etc/apache2/envvars）に、AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEYを
    セットしておくこと。当該情報をアカウント情報として利用する。
    """
    _conn = None
    _bucket_name = settings.BUCKET        # デフォルト値。変更可。
    _bucket = None
    _n_threads = 20
    def __init__(self):
        self._conn = S3Connection(aws_access_key_id=settings.ACCESS_KEY,
                                 aws_secret_access_key=settings.SECRET_KEY,
                                 host=settings.DOMAIN,
                                 is_secure=settings.IS_SECURE)
        self._bucket = self.get_or_insert_bucket(self._bucket_name)
        print "connect %s of S3 success." % self._bucket
        today = datetime.now()
        self._today = datetime(today.year, today.month, today.day)    # 時刻部分を0:00:00に揃える。

    @property
    def conn(self):
        return self._conn
    
    def set_bucket_name(self, bucket_name):
        self._bucket_name = bucket_name

    def get_all_buckets(self):
        return list(self._conn)

    def get_all_files(self):
        """
        self._bucketで指定されている全ファイルを取得する。
        """
        files = []
        for k in self._bucket.get_all_keys():
            fkey = k.name
            files.append(fkey)
        return files

    def get_fkey_dirpath(self, fkey):
        """
        fkeyからファイル名のみを削除したディレクトリパスを返す。
        """
        sPos = fkey.rfind("/")
        if ( sPos == -1 ):
            return None
        return fkey[:sPos]

    def get_fkey_date(self, fkey):
        """
        fkeyから日時情報を取得し、datetime型で返す。
        """
        sPos = fkey.rfind("/")
        if ( sPos == -1 ):
            return None
        date_s = fkey[sPos+1:].replace(".json", "")
        return datetime.strptime(date_s, "%Y%m%d")

    def delete_old(self, expire_day=5):
        """
        expire_dayで指定された日数分経過している古いファイルを消す。
        expire_day=0は、過去のものすべてを削除する。
        expire_day=1は、今日が2013/2/3とすると、2013/2/2以前のものを削除する。
        """
        today = self._today
        for fkey in self.get_all_files():
            created_date = self.get_fkey_date(fkey)
            if ( created_date == None ):
                continue
            if ( created_date + timedelta(days=expire_day) <= today ):
                self.delete(fkey)
    
    def get_bucket(self, bucket_name):
        return self._conn.get_bucket(bucket_name)
    
    def get_or_insert_bucket(self, bucket_name):
        """
        bucketを作成。bucketがあれば既存のものを返す。
        """
        if ( self._bucket ):
            # caching
            return self._bucket
        
        if bucket_name in self._conn:
            bucket = self.get_bucket(bucket_name)
        else:
            bucket = self._conn.create_bucket(bucket_name, location=Location.APNortheast)
        self._bucket = bucket
        
        return self._bucket

    def set_key(self, bucket, fkey):
        k = Key(bucket)
        k.key = fkey
        return k
    
    def insert_file(bucket, fkey, content_fp):
        """
        新規作成メソッド。
        """
        k = self.set_key(bucket, fkey)
        # k.set_contents_from_string(content)
        k.set_contents_from_stream(content_fp)
        return k

    def get_or_insert_file(self, bucket, fkey, content_fp):
        """
        更新メソッド。既存の場合は既存オブジェクトを返す。
        """
        k = self.set_key(bucket, fkey)
        is_exists = k.exists()
        if not ( is_exists ):
            # 新規作成
            print "creating... ",
            k.set_contents_from_stream(content_fp, replace=True)
        else:
            # 既存更新
            print "updating... ",
            k.set_contents_from_stream(content_fp, replace=True)
        print "Done."
            
        return k, is_exists

    def store_content(self, fkey, content_fp):
        """
        fkeyにcontentを新規保存。
        新規作成の場合のみ作成する。fkeyが既存の場合は何もしない。
        作成成功したらTrue。それ以外はFalseを返す。
        """
        try:
            # bucket("lafla.co.jp")を取得
            bucket = self.get_or_insert_bucket(self._bucket_name)
            # fkeyにcontentを保存
            k = self.insert_file(bucket, fkey, content_fp)
            is_success = True
        except Exception, e:
            is_success = False

        return not is_success

    def get_chunks(self, fsize):
        chunks = []
        min_block_size = settings.MULTI_UPLOAD_THREASHOLD_SIZE
        if ( fsize <= min_block_size ):
            # 5mb未満の場合はmulti-part転送しない
            chunks.append( (0, fsize) )
        else:
            # 5mb以上の場合
            spos = 0
            size = 0
            n_blocks = fsize / min_block_size
            if ( n_blocks > self._n_threads ):
                # 5MBずつに分割した場合に最大スレッド数を超える場合はスレッド数で全体のサイズを等分割する
                n_chunks = self._n_threads
                unit_size = fsize / self._n_threads
            else:
                # 5MBずつに分割した場合に最大スレッド数以下となる場合
                n_chunks = n_blocks
                unit_size = min_block_size
                
            for i in range(n_chunks):
                if ( i == (n_chunks - 1) ):
                    chunks.append( (spos, None) )
                else:
                    chunks.append( (spos, unit_size) )
                spos += unit_size

        return chunks

    def update_content(self, fkey, fn, fsize):
        """
        fkeyにfnを保存。
        新規作成・更新の場合のみTrue。fkeyが既存の場合は更新する。
        更新成功したらTrue。それ以外はFalseを返す。
        """
        # bucket("lafla.co.jp")を取得
        bucket = self._bucket
        # multi part upload
        mp = bucket.initiate_multipart_upload(fkey)
        threads = []
        no = 1
        for spos, this_size in self.get_chunks(fsize):
            th = Uploader(mp, fn, no, spos, this_size)
            th.setDaemon(True)
            th.start()
            threads.append(th)
            no += 1

        timeout = 1800                  # タイムアウト時間(sec)
        n_th = len(threads)
        while ( len(threads) > 0 ):
            th = threads.pop()                  # 末尾を取得
            try:
                if ( th.isAlive() ):
                    threads.insert(0, th)       # 先頭に戻す
                    time.sleep(1)
                else:
                    # スレッドが終了している場合
                    if ( th in threads ):
                        threads.remove(th)
            except KeyboardInterrupt:
                print "Ctrl-c received. Sending kill signal to threads..."
                raise   # 上位でキャッチ

        mp.complete_upload()            # S3ファイルが生成される
        print "   All Threads Upload Done."

        return True

    def delete(self, fkey):
        """
        fkeyで指定したファイルの削除
        """
        k = self.set_key(self._bucket, fkey)
        self._bucket.delete_key(k)
        print "[DEL] %s" % ( fkey )

    def get_content(self, bucket, fkey):
        """
        キー（fkey）で指定されたファイルのコンテンツをS3から取得。
        """
        k = self.set_key(bucket, fkey)
        return k.get_contents_as_string()

    def store_file(self, bucket, fkey, fpath):
        """
        キー（fkey）で指定されたファイルをfnに保存。
        """
        k = self.set_key(bucket, fkey)
        k.get_contents_to_filename(fpath)

    def get_file(self, fkey, fpath):
        """
        受け手側でclose()すること。
        """
        k = self.set_key(self._bucket, fkey)
        k.get_contents_to_filename(fpath)
        ifp = open(fpath, "r")
        return ifp


def transaction_enter(using):
    """
    トランザクション処理を開始する。それまでのcommit漏れは直前でcommitしておく。
    """
    transaction.commit_unless_managed(using=using)
    transaction.enter_transaction_management(using=using)
    transaction.managed(True, using=using)


def transaction_leave(using):
    transaction.leave_transaction_management(using=using)


def transaction_commit(using):
    """
    DB(using)がdirtyな場合、commitし、トランザクション区間を終了する。
    """
    if ( transaction.is_managed(using=using) ):
        if ( transaction.is_dirty(using=using) ):
            transaction.commit(using=using)
        transaction.leave_transaction_management(using=using)
    

def transaction_rollback(using):
    """
    DB(using)をロールバックし、トランザクション区間を終了する。
    """
    if ( transaction.is_managed(using=using) ):
        transaction.rollback(using=using)
        transaction.leave_transaction_management(using=using)


class Restore(object):
    def __init__(self, use_transaction=True, excludes=[], using=DEFAULT_DB_ALIAS):
        self._excludes = excludes
        self._s3 = S3()
        self._commit = use_transaction
        self._using = using
        self._base_dir = settings.DIRECTORY
        self._projname = self.get_project_name()
        # アプリ参照関係を調べて依存していないテーブルから順に処理できるリストを作成
        app_list = SortedDict((app, None) for app in get_apps() if app not in excludes)
        #self._models = sort_dependencies(app_list.items())
        #self._models.reverse()
        self._models = sort_relation_dependencies(app_list)
        self._db = DEFAULT_DB_ALIAS

    def get_project_name(self):
        project_root = settings.PROJECT_ROOT
        sPos = project_root.rfind("/")
        if ( sPos != -1 ):
            return project_root[sPos+1:]
        return None

    def flush_all_tables(self):
        """
        全テーブル内容を削除する。
        ToDo: そもそもDjangoに備わっているJSON形式のレストアバッチコマンドを流用してもよい。
        """
        #flush_cmd = flush.Command()
        #flush_cmd.execute()
        connection = connections[self._db]
        style = no_style()
        for app_name in settings.INSTALLED_APPS:
            try:
                import_module('.management', app_name)
            except ImportError:
                pass
        sql_list = sql_flush(style, connection, only_django=True)
        confirm = True
        if ( confirm ):
            try:
                cursor = connection.cursor()
                print "[Start Deleting]..."
                for sql in sql_list:
                    print "%s... " % sql,
                    cursor.execute(sql)
                    print "Done."
                print "[Done] delete tables completely!"
            except Exception, e:
                transaction.rollback_unless_managed(using=self._db)
                raise CommandError("""Database %s couldn't be flushed. Possible reasons:
  * The database isn't running or isn't configured correctly.
  * At least one of the expected database tables doesn't exist.
  * The SQL was invalid.
Hint: Look at the output of 'django-admin.py sqlflush'. That's the SQL this command wasn't able to run.
The full error: %s""" % (connection.settings_dict['NAME'], e))
            transaction.commit_unless_managed(using=self._db)

    '''
    def s3_to_database(self):
        try:
            if ( self._commit ):
                # start transaction
                transaction_enter(self._using)
            # restore all tables
            self._s3_to_database()
            # success and commit. end transaction.
            if ( self._commit ):
                transaction_commit(self._using)
        except (SystemExit, KeyboardInterrupt):
            raise
        except Exception:
            if ( self._commit ):
                # rollback
                transaction_rollback(self._using)
            raise
    '''

    def s3_to_database(self):
        # restore all tables
        try:
            transaction_enter(using=self._using)
            self._s3_to_database()
        finally:
            transaction_leave(using=self._using)

    def _s3_to_database(self):
        """
        S3からテーブルを取得し、database中の対応するテーブルにレコードを書き込む。
        """
        s3_files = self._s3.get_all_files()
        latest_files = self.latest_ordered_files(s3_files)
        # 全テーブルの復元
        # ToDo: latest_filesを一連のリレーションツリーとしてその単位でコミットできるようにする。
        # つまり、latest_filesを要素としたリストを作成。その単位で以下の処理をループでまわす。
        try:
            self._restore_tables(latest_files)
            transaction.commit(using=self._using)
        except Exception, e:
            print "[ERROR] %s" % str(e)
            transaction.rollback(using=self._using)
        '''
        else:
            transaction.commit()
        '''

    def get_model_path(self, model):
        """
        modelインスタンスから対応するパスを取得して返す。
        ディレクトリパスだけで、ファイル名については各自対応すること。
        """
        clsname = "%s.%s" % ( model.__module__, model.__name__ )
        model_path = "%s/%s/%s/apps/%s" % ( self._base_dir, self._using, self._projname, clsname )
        return model_path

    def latest_ordered_files(self, files):
        """
        filesの中からmodelインスタンスに対応した最新のファイルパス名を取得する。
        依存関係も考慮して、依存していない順番で、かつ、同一モデルに対するバックアップ済みJSONファイルが
        ある場合は最新のもののみをリストとして返す。
        """
        latest_files = []
        fkeys = []
        init_date = datetime.strptime("19700101", "%Y%m%d")
        for model in self._models:
            model_path = self.get_model_path(model)
            for fkey in files:
                fkey_path = self._s3.get_fkey_dirpath(fkey)
                if ( model_path == fkey_path ):
                    fkeys.append(fkey)
            latest = init_date
            for fkey in fkeys:
                fkey_date = self._s3.get_fkey_date(fkey)
                if ( fkey_date > latest ):
                    latest = fkey_date
            fkeys = []
            if ( latest == init_date ):
                # the model does not exist in S3...
                continue
            latest_files.append( "%s/%s.json" % (model_path, latest.strftime("%Y%m%d")) )

        return latest_files

    def save_list(objs):
        for obj in objs:
            obj.object.save_base(cls=obj.object.__class__, origin=None,    # signalを発行しない
                                 raw=True, using=self._using, force_insert=True)
        return
    
    def _random_store(failed_fkeys):
        """
        ランダムにモデルを選択し、成功するまでsaveを続ける。
        """
        failed = None
        while failed_fkeys:
            # choice one table randomly.
            table_fkey = random.choice(failed_fkeys)
            failed_fkeys.remove(table_fkey)
            if ( failed ):
                failed_fkeys.append(failed)
            print "Restarting[2] <%s> ..." % table_fkey
            stream = self._s3.get_file(table_fkey, "./restore.json")
            for obj in StreamDeserializer(stream, line_by_line=True):
                try:
                    obj.object.save_base(cls=obj.object.__class__, origin=None,    # signalを発行しない
                                         raw=True, using=self._using, force_insert=True)
                except psycopg2.IntegrityError, e:
                    # 末尾に戻す
                    print "[FAILED] %s" % obj
                    failed = table_fkey
                    break
        return

    def _reverse_store(failed_fkeys):
        """
        逆順にモデルを選択し、成功するまでsaveを続ける。
        """
        fails = []
        for table_fkey in reversed(failed_fkeys):
            # choice one table randomly.
            print "Restarting[1] <%s> ..." % table_fkey
            stream = self._s3.get_file(table_fkey, "./restore.json")
            for obj in StreamDeserializer(stream, line_by_line=True):
                try:
                    obj.object.save_base(cls=obj.object.__class__, origin=None,    # signalを発行しない
                                         raw=True, using=self._using, force_insert=True)
                except psycopg2.IntegrityError, e:
                    # 末尾に戻す
                    print "[FAILED] %s" % obj
                    fails.append(failed)
                    break
        return

    def _restore_tables(self, table_fkeys):
        # failed_objs = []
        failed_models = []
        cnt = 1
        for table_fkey in table_fkeys:
            print "[%d]Starting <%s> ..." % ( cnt, table_fkey )
            stream = self._s3.get_file(table_fkey, "./restore.json")
            print "   [Done] JSON file got from S3."

            success_cnt, unit = 0, 10000
            for obj in StreamDeserializer(stream, line_by_line=True):
                try:
                    obj.object.save_base(cls=obj.object.__class__, origin=None,    # signalを発行しない
                                         raw=True, using=self._using, force_insert=True)
                    success_cnt += 1
                    if ( success_cnt % unit == 0 ):
                        print "   %d records are restored..." % success_cnt
                except psycopg2.IntegrityError, e:
                    # failed_objs.append(obj)
                    # ToDo: 参照先のテーブルのpkがあるかどうか調べて、
                    # なければ参照先のテーブルを先にappendする。
                    # see load_recursive(): http://djangosnippets.org/snippets/167/
                    if not table_fkey in failed_models:
                        failed_models.append(table_fkey)
                    print "[FAILED] %s" % str(e)
            print "   [Done] RESTORE table."
            cnt += 1

        '''
        if ( failed_objs ):
            self.save_list(failed_objs)
        '''
        if ( failed_models ):
            # if failed, retry recursive call...
            failed_models = self._reverse_store(failed_models)
            if ( failed_models ):
                self._random_store(failed_models)

        print "   [Done] All Tables"

