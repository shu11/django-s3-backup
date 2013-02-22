# -*- coding: utf-8 -*-
from django.utils import simplejson

class xload(object):
    """
    jsonオブジェクトのリストを返すイテレータークラス。
    """
    def __init__(self, stream_or_string):
        if ( isinstance(stream_or_string, basestring) ):
            self._iter = stream_or_string.striplines()
        else:
            self._iter = stream_or_string.xreadlines()

    def __iter__(self):
        return self

    def next(self):
        return simplejson.loads( self._iter.next() )
