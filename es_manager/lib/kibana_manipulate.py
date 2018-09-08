from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
es = Elasticsearch('elasticsearch')


def rm_iso_search():
    # Loop through all visualization
    vis_all_query = Search(using=es, index='.kibana', doc_type='visualization').query('match_all').scan()
    savedsearch_in_vis_list = list()
    for item in vis_all_query:
        if hasattr(item, 'savedSearchId'):
            if item.savedSearchId not in savedsearch_in_vis_list:
                savedsearch_in_vis_list.append(item.savedSearchId)

    savedsearch_list = list()
    ssearch_all_query = Search(using=es, index='.kibana', doc_type='search').query('match_all').scan()
    for item in ssearch_all_query:
        savedsearch_list.append(item.meta.id)
    for item in savedsearch_list:
        if item not in savedsearch_in_vis_list:
            es.delete(index='.kibana', doc_type='search', id=item)


def rm_iso_vis():
    dboard_all_query = Search(using=es, index='.kibana', doc_type='dashboard').query('match_all').scan()
    vis_list_in_dboard = list()
    for item in dboard_all_query:
        vis_list = eval(str(item.panelsJSON))
        for vis in vis_list:
            if vis['id'] not in vis_list_in_dboard:
                vis_list_in_dboard.append(vis['id'])
    print vis_list_in_dboard

    # Loop through all visualization
    vis_all_query = Search(using=es, index='.kibana', doc_type='visualization').query('match_all').scan()
    vis_list_full = list()
    for item in vis_all_query:
        vis_list_full.append(item.meta.id)

    for item in vis_list_full:
        if item not in vis_list_in_dboard:
            print 'Removing visualization with id ' + item
            es.delete(index='.kibana', doc_type='visualization', id=str(item))
