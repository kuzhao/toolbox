from elasticsearch import Elasticsearch, helpers
import threading


class ESManipulate:
    def __init__(self, es_clienthost):
        self.es_clienthost = es_clienthost
        self.index_list = dict()
        self.template_list = list()

        tmp_list = self.es_clienthost.cat.indices().split('\n').pop()
        for tmp in tmp_list:
            line_list = tmp.rstrip().split()
            self.index_list[line_list[2]] = {"health": line_list[0], "state": line_list[1]}
        self.template_list = self.es_clienthost.indices.get_template().keys()

    def combine_index(self, src_idx_list, dst_idx):

        def reindex_func(src_idx, dst_idx):
            es = Elasticsearch(self.es_clienthost)
            try:
                helpers.reindex(es, src_idx, dst_idx)
            except:
                print('Reindex failed for src_idx '+src_idx+' into dst_idx'+dst_idx)
        thread_pool = list()
        for src_idx in src_idx_list:
            thread_pool.append(threading.Thread(target=reindex_func, args=(src_idx, dst_idx,)))
        for thread in thread_pool:
            thread.start()
        # Wait for thread to
        while True:
            if not threading.active_count():
                break
