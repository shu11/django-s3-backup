# -*- coding: utf-8 -*-
from django.utils.datastructures import SortedDict
from django.db.models import get_app, get_apps, get_model, get_models
from django.core.management.commands.dumpdata import sort_dependencies

def test_main():
    app_list = SortedDict((app, None) for app in get_apps())
    models = sort_relation_dependencies(app_list)
    return models


def get_relation_models(model):
    """
    モデルのリレーション（OneToOneField, Foreignkey, ManyToManyField）を検出し、
    リストで返す。リストの順番は検出順となる。
    """
    relation_to, deps = [], []
    for field in model._meta.fields:
        if ( hasattr(field.rel, "to") ):
            # save the OneToOneField or Foreignkey relation model.
            rel_model = field.rel.to
            if ( hasattr(rel_model, 'natural_key') ):
                deps.append(rel_model)
            else:
                relation_to.append(rel_model)
    for field in model._meta.many_to_many:
        if ( hasattr(field.rel, "to") ):
            # save the ManyToManyField relation model.
            rel_model = field.rel.to
            if ( hasattr(rel_model, 'natural_key') ):
                deps.append(rel_model)
            else:
                relation_to.append(rel_model)

    # natural keyを持つリレーション先の方が先に優先される。（不要かも）
    relation_to = deps + relation_to
    return relation_to


def _sort_relation_order(model, model_list, visited):
    """
    グラフを幅優先探索する。
    リレーション先を持たないモデルから順にmodel_listに挿入する。
    - model: 探索するモデルオブジェクト
    - model_list: 参照先の依存関係を解決したリスト（引数渡しで値更新）
    - visited: 訪問済みモデルリスト（引数渡しで値更新）

    notice:
        循環リレーションがある場合はどのモデルが最初に生成されるのか分からないので
        根本的に対応不能。
        その場合は、探索済みモデルが検出された時点で返却モデル配列に追加される。
        つまり、循環モデルパスの最初に訪問したものから逆順で挿入される。
    """
    visited.append(model)
    relation_to = get_relation_models(model)
    if not ( relation_to ):
        # 参照先がなければテーブル作成OK
        if not ( model in model_list ):
            model_list.append(model)
            #model_list.append(None)     # ToDo: mark reference end
            #print "[1]insert to model_list for %s" % model
    else:
        for rel_model in relation_to:
            if not ( rel_model in visited ):
                _sort_relation_order(rel_model, model_list, visited)
            else:
                # if rel_model has already visited, stop recursion.
                if not ( rel_model in model_list ):
                    model_list.append(rel_model)
                    # model_list.append(None)     # ToDo: mark reference end
                    #print "[2]insert to model_list for %s" % model
                else:
                    # do-nothing
                    pass
        if not ( model in model_list ):
            model_list.append(model)
            #print "[3]insert to model_list for %s" % model
            
    return


def sort_relation_dependencies(app_list):
    models = sort_dependencies(app_list.items())
    models.reverse()
    #print "(before) #models: %d" % len(models)
    model_list, visited = [], []
    while models:
        model = models.pop(0)
        _sort_relation_order(model, model_list, visited)
    #print "(after) #models: %d" % len(model_list)
    return model_list
