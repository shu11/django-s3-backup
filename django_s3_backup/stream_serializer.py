# -*- coding: utf-8 -*-
try:
    from StringIO import StringIO
except ImportError:
    pass
from django.core.serializers.base import DeserializationError
from django.core.serializers.json import Serializer as JsonSerializer
from django.core.serializers.json import Deserializer as JsonDeserializer
from django.core.serializers.json import DjangoJSONEncoder
from django.core.serializers.python import Deserializer as PythonDeserializer
from django.utils.encoding import smart_unicode, is_protected_type
from django.utils import simplejson
import xsimplejson
import sys, os


class SlowStream(object):
    """
    querysetのリストを返すイテレータークラス。
    """
    UNIT = 1000
    def __init__(self, queryset):
        self._queryset = queryset.order_by("pk")        # 順序を確定させるために並び替え
        self._rest = 0
        self._start_pos = 0
        self.all = self._queryset.count()
        self.all_fetchs = (self.all / self.UNIT) + 1
        self.n_fetchs = 0
        self.cnt = 0
        self.n_alldots = 30
        print "   ",
        self._buf = self.fetch()

    def fetch(self):
        epos = self._start_pos + self.UNIT
        recs = self._queryset[self._start_pos:epos]
        self._rest = recs.count()
        if ( self._rest == 0 ):
            return []
        # dbg start
        #print "   spos: %d -> %d, epos: %d, _rest: %d, n_fetchs: %d" %\
        #      ( self._start_pos, self._start_pos + self._rest, epos, self._rest, self.n_fetchs )
        # dbg end
        # print "   obj.id: %d" % recs[0].id    # for dbg

        self._start_pos += self._rest
        self.n_fetchs += 1
        ratio = self._rest / float(self.all)
        if ( ratio >= 1 ):
            ratio = 1
        n_dots = "#" * int( self.n_alldots * ratio )
        sys.stdout.write(n_dots)
        sys.stdout.flush()
        # os.fsync(sys.stdout.fileno())         # cannot use this because it causes invalid argument error.
        
        return list(recs)

    def next(self):
        if ( self._rest == 0 ):
            self._buf = self.fetch()

        # リストの先頭を取得
        try:
            rec = self._buf.pop(0)
            self._rest -= 1
            self.cnt += 1
            return rec
        except IndexError:
            # リストが空になったらイテレーション終了
            raise StopIteration

    def __iter__(self):
        return self


class Stream(object):
    """
    querysetのリストを返すイテレータークラス。
    """
    UNIT = 10000
    def __init__(self, queryset):
        self._queryset = queryset
        self._rest = 0
        self._sum = 0
        self._start_pos = 0
        self.all = self._queryset.count()
        self.all_fetchs = (self.all / self.UNIT) + 1
        self.n_fetchs = 0
        self.cnt = 0
        self.n_alldots = 30
        print "   ",
        self._buf = self.fetch()

    def fetch(self):
        epos = self._start_pos + self.UNIT
        recs = self._queryset.filter(pk__gte=self._start_pos, pk__lt=epos)
        self._rest = recs.count()
        self._sum += self._rest         # 取得済み合計数をカウント
        if ( (self._rest == 0) and (self._sum == self.all) ):
            # 全レコードを取得できたら終了
            return []

        self._start_pos += self.UNIT
        self.n_fetchs += 1
        ratio = self._rest / float(self.all)
        if ( ratio >= 1 ):
            ratio = 1
        n_dots = "#" * int( self.n_alldots * ratio )
        sys.stdout.write(n_dots)
        sys.stdout.flush()
        # os.fsync(sys.stdout.fileno())         # cannot use this because it causes invalid argument error.
        
        return list(recs)

    def next(self):
        if ( self._rest == 0 ):
            self._buf = self.fetch()

        # リストの先頭を取得
        try:
            rec = self._buf.pop(0)
            self._rest -= 1
            self.cnt += 1
            return rec
        except IndexError:
            # リストが空になったらイテレーション終了
            raise StopIteration

    def __iter__(self):
        return self


class Serializer(JsonSerializer):
    """
    stream converter from a record  to json.
    """
    cnt = 0
    allcnt = 0
    def serialize(self, queryset, **options):
        """
        Serialize a queryset.
        """
        self.options = options

        # self.stream = options.pop("stream", StringIO())       # python2.6ベースの場合は元に戻す
        self.stream = options.pop("stream", None)
        self.selected_fields = options.pop("fields", None)
        self.use_natural_keys = options.pop("use_natural_keys", False)
        self.line_by_line = options.pop("line_by_line", False)

        self.start_serialization()
        self.allcnt = queryset.count()
        self._current = {}
        # print "[%s]" % queryset.model.__name__
        for obj in Stream(queryset):
            # print "obj.id: %d" % obj.id       # for dbg
            self.start_object(obj)
            # Use the concrete parent class' _meta instead of the object's _meta
            # This is to avoid local_fields problems for proxy models. Refs #17717.
            # concrete_model = obj._meta.concrete_model
            # for field in concrete_model._meta.local_fields:
            for field in obj._meta.local_fields:
                if field.serialize:
                    if field.rel is None:
                        if self.selected_fields is None or field.attname in self.selected_fields:
                            self.handle_field(obj, field)
                    else:
                        if self.selected_fields is None or field.attname[:-3] in self.selected_fields:
                            self.handle_fk_field(obj, field)
            # for field in concrete_model._meta.many_to_many:
            for field in obj._meta.many_to_many:
                if field.serialize:
                    if self.selected_fields is None or field.attname in self.selected_fields:
                        self.handle_m2m_field(obj, field)
            self.end_object(obj)
        self.end_serialization()
        print
        return self.getvalue()

    def start_serialization(self):
        if not ( self.line_by_line ):
            self.stream.write("[")
        
    def end_serialization(self):
        """
        overwrite do-nothing.
        json.Serializer()では、ここでself.objectsをsimplejson.dump()しているのでフタ閉め。
        """
        self.options.pop('stream', None)
        self.options.pop('fields', None)
        self.options.pop('use_natural_keys', None)
        if not ( self.line_by_line ):
            self.stream.write("]")

    '''
    def start_object(self, obj):
        # print "[%s]" % smart_unicode(obj._meta)
        pass
    '''
        
    def end_object(self, obj):
        """
        overwrite do-nothing.
        python.Serializer()では、ここでself.objectsにself._currentをappend()しているのでフタ閉め。
        """
        objct = {
            "model"  : smart_unicode(obj._meta),
            "pk"     : smart_unicode(obj._get_pk_val(), strings_only=True),
            "fields" : self._current
            }
        simplejson.dump(objct, self.stream, cls=DjangoJSONEncoder, **self.options)
        self.cnt += 1
        if ( self.line_by_line ):
            # １行１レコード形式
            self.stream.write("\n")
        else:
            if ( self.cnt < self.allcnt ):
                self.stream.write(",")
        self._current = {}
    
    def handle_field(self, obj, field):
        """
        １つのレコードを辞書オブジェクト化してjsonダンプする。
        """
        value = field._get_val_from_obj(obj)
        # Protected types (i.e., primitives like None, numbers, dates,
        # and Decimals) are passed through as is. All other values are
        # converted to string first.
        if is_protected_type(value):
            self._current[field.name] = value
        else:
            self._current[field.name] = field.value_to_string(obj)
        

    def handle_fk_field(self, obj, field):
        if self.use_natural_keys and hasattr(field.rel.to, 'natural_key'):
            related = getattr(obj, field.name)
            if related:
                value = related.natural_key()
            else:
                value = None
        else:
            value = getattr(obj, field.get_attname())
        self._current[field.name] = value
        

    def handle_m2m_field(self, obj, field):
        if field.rel.through._meta.auto_created:
            if self.use_natural_keys and hasattr(field.rel.to, 'natural_key'):
                m2m_value = lambda value: value.natural_key()
            else:
                m2m_value = lambda value: smart_unicode(value._get_pk_val(), strings_only=True)
            self._current[field.name] = [m2m_value(related)
                               for related in getattr(obj, field.name).iterator()]


def Deserializer(stream_or_string, **options):
    """
    JSON版のDeserializerに、line_by_line機能を追加したもの。
    本ファイルのSerializerに対応したdeserializerクラス。
    以下のようにしてコールする：
    >>> from django_s3_backup.stream_serializer import *
    >>> from django_s3_backup.xsimplejson import *
    >>> ifp = open("backup.test2", "r")
    >>> d = Deserializer(ifp, line_by_line=True)
    """
    line_by_line = options.get("line_by_line", False)
    if not ( line_by_line ):
        # 一行１レコード形式ではない場合はオリジナルに任せる
        JsonDeserializer(stream_or_string, **options)
        return

    for obj in PythonDeserializer(xsimplejson.xload(stream_or_string), **options):
        yield obj

