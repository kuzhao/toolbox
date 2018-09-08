#!/usr/bin/env python

from elasticsearch import Elasticsearch, helpers
from fnmatch import fnmatch
import argparse
import exceptions
import re
import traceback
import sys
import json
sys.path.insert(0, '/opt/pixar/Tractor-2.2/lib/python2.7/site-packages')
# import tractor.api.query as tq


# This function gets current indices in list form in the cluster
def _getinfo(option):
    global es
    # es.cat.indices() will return with a long string containing status of all indices in cluster,
    # which appears to be a good start where to grab target information about 1 or more indices
    option_map = {'idx_name': 2, 'health': 0, 'state': 1}
    if option not in option_map:
        print 'Invalid option: ' + option + ' for getinfo.'
        return
    tmp_list = es.cat.indices().split('\n')
    # Return empty list if es.cat returned empty list result
    if not tmp_list:
        print 'No index was found in this cluster.'
        return
    tmp_list.pop()
    result = list()
    for item in tmp_list:
        tmp_list1 = str(item).rstrip().split()
        if tmp_list1[1] == 'open':
            result.append(tmp_list1[option_map[option]])
        else:
            continue
    return result


# check_field: Check if a field exists in one index/doc_type
def _check_field(field_name, index, doc_type):
    src_field_list = field_name.split('.')
    field_mapping = es.indices.get_mapping(index=index)[unicode(index)]['mappings'][unicode(doc_type)][unicode('properties')]
    if len(src_field_list) == 1:  # plain, one dim data field
        if src_field_list[0] not in field_mapping.keys():
            print 'Field not found.'
            return False
    else:  # multidim data field in json
        # design recursive walk through field_map json
        def walk(json_in_list, json_tosearch):
            for item in json_tosearch:
                if len(json_in_list) > 1:
                    if json_in_list[0] == item:
                        walk(json_in_list[1:len(json_in_list)], json_tosearch[item]['properties'])
                        return
                else:
                    if json_in_list[0] == item:
                        return
            raise exceptions.EOFError
        try:
            walk(src_field_list, field_mapping)
        except exceptions.EOFError:
            print 'field ' + field_name + ' not found'
            return False
    return True


def _get_field_value(doc, field):
    if len(field) == 1:
        return doc.pop(field[0])
    else:
        return _get_field_value(doc[field[0]], field[1:])


def reindex(**kwargs):
    global es
    src_idx = kwargs['src_idx']
    dst_idx = kwargs['dst_idx']
    idx_list = _getinfo('idx_name')

    if src_idx not in idx_list:
        print 'Source index ' + src_idx + ' not found.'
        raise exceptions.AssertionError
    # Get mappings of src index
    src_mappings = es.indices.get_mapping(index=src_idx)[unicode(src_idx)]
    if args.debug:
        print 'Mapping info of src index:'
        for type_name in src_mappings[unicode('mappings')].keys():
            print 'Doc Type: ' + type_name
            tmp_dict = src_mappings[unicode('mappings')][unicode(type_name)][unicode('properties')]
            for field in tmp_dict.keys():
                print field + ': ' + str(tmp_dict[field])
        print ''
    try:
        es.reindex(body='{"source":{"index":"'+src_idx+'"},"dest":{"index":"'+dst_idx+'"}}', request_timeout=4000)
    except:
        print 'Cannot reindex src index to target.'
        if args.debug:
            traceback.print_exc()
        raise exceptions.AssertionError


def devalue(**kwargs):
    global es
    doc_type = kwargs['doc_type']
    src_field = kwargs['src_field']
    op = raw_input('What to do with this data field?\n')
    # Validate src field name
    try:
        if not _check_field(src_field, kwargs['index'], doc_type):
            return
    except:
        print 'Index/DocType value error.'
        if args.debug:
            traceback.print_exc()
        return
    doc = helpers.scan(es, query='{"query":{"constant_score":{"filter":{"exists":{"field":"'+src_field+'"}}}}}',
                       index=kwargs['index'], doc_type=doc_type)
    # Check operator
    valid_op_list = ['*', '/', '+', '-']
    operator = ''
    factor = ''
    for item in valid_op_list:
        if item in op:
            factor = re.findall(item+'\s*(\d[\d+\.]+)', op)
            operator = item
            break

    if 'factor' not in locals() or not factor or not operator:
        print 'Invalid arithmetic operation'
        return
    factor = factor[0]
    global flag

    def calculator(value, operator, factor):
        mapping = {'*': value*factor,
                   '/': value/factor,
                   '+': value+factor,
                   '-': value-factor}
        return mapping[operator]

    def recur_changevalue(field, entry, operator, factor):
        global flag
        if len(field) == 1:
            if entry[field[0]] >= 1:
                entry[field[0]] = calculator(entry[field[0]], operator, float(factor))
                flag = True
        else:
            entry[field[0]] = recur_changevalue(field[1:], entry[field[0]], operator, factor)
        return entry
    for record in doc:
        flag = False
        result = recur_changevalue(src_field.split('.'), record['_source'], operator, factor)
        if flag:
            es.index(index=kwargs['index'], doc_type=doc_type, id=record['_id'], body=result)
        else:
            continue


def addfield(**kwargs):
    src_field = kwargs['src_field']
    target_field = kwargs['dst_field']
    index = kwargs['index']
    doc_type = kwargs['doc_type']
    tq.setEngineClientParam(user='tq_user', password="hahahahahaha")
    # Validate src field name
    if src_field:
        try:
            if not _check_field(src_field, index, doc_type):
                return
        except:
            print 'Index/DocType value error.'
            if args.debug:
                traceback.print_exc()
            return
    doc = helpers.scan(es, query='{"query":{"not":{"exists":{"field":"'+target_field+'"}}}}',
                       index=index, doc_type=doc_type)

    def transform(name_string):
        result = dict()
        for idx in kwargs['ref_idx_list']:
            try:
                result = es.get(index=idx, id=name_string['command_id'], doc_type='default')
                break
            except:
                pass
        if not result:
            return
        return result['_source']['rsvMem']
    for item in doc:
        docid = item['_id']
        source_doc = item['_source']
        if target_field not in source_doc:
            # Fill codes here about how the value of target_field should be derived from existing fields
            result = transform(source_doc)
            if result:
                source_doc[target_field] = result
                es.index(index=index, doc_type=doc_type, id=docid, body=source_doc)


def fieldop(**kwargs):
    global es
    doc_type = kwargs['doc_type']
    src_field = kwargs['src_field']
    if kwargs['dst_field']:
        dst_field = kwargs['dst_field']
        del_flag = False
    else:
        del_flag = True
    # Create doc scanner
    doc_scanner = helpers.scan(es, query='{"query":{"exists":{"field":"'+src_field+'"}}}',
                               index=kwargs['index'], doc_type=doc_type)

    def recur_add(doc, field, value):
        if len(field) == 1:
            doc[field[0]] = value
            return doc
        else:
            doc[field[0]] = recur_add(doc[field[0]], field[1:], value)
        return doc
    # delete src_field if del_flag true
    if del_flag:
        def recur_find(doc):
            for key in doc:
                if type(doc[key]) == dict:
                    recur_find(doc[key])
                else:
                    if 'checked' == key:
                        raise exceptions.StandardError
            return

        def recur_changekey(doc):
            tmp_doc = doc
            for key in doc:
                if type(doc[key]) == dict:
                    doc[key] = recur_changekey(doc[key])
                else:
                    if 'checked' == key:
                        tmp_doc.pop(key)
                        break
            return tmp_doc
        for item in doc_scanner:
            try:
                recur_find(item['_source'])
            except:
                tmp = recur_changekey(item['_source'])
                es.index(index=kwargs['index'], doc_type=doc_type, id=item['_id'], body=tmp)
        return

    # Validate src field
    if not _check_field(src_field, kwargs['index'], doc_type):
        print 'Source field does not exist'
        return
    # Halt if dst_field name is identical with src_field
    if dst_field == src_field:
        print 'The name of new field is the same with old one'
        return
    dst_field = dst_field.split('.')
    src_field = src_field.split('.')
    for item in doc_scanner:
        tmp_value = _get_field_value(item['_source'], src_field)
        # Renaming field
        print recur_add(item['_source'], dst_field, tmp_value)


def renew_idx_template(tmpl_name, file_path):
    with open(file_path) as f:
        template_body = json.load(f)
    es.indices.put_template(name=tmpl_name, body=template_body[template_body.keys()[0]])

# Init argparse
parser = argparse.ArgumentParser(description='Elasticsearch DB toolbox')
parser.add_argument('-nodes', metavar='ES-NODES', type=list, help='target elasticsearch node list',
                    default=['elasticsearch'])
parser.add_argument('-A', '--action', metavar='ACTION', type=str,
                    help='Op to perform, choose from:fieldop, mergeidx, devalue, addfield, puttemp',
                    required=True, choices=['fieldop', 'mergeidx', 'devalue', 'addfield', 'puttemp'])
parser.add_argument('-D', '--debug', help='Toggle debug info display',
                    action='store_true', default=False)

args = parser.parse_args()
# Init vars
node_list = args.nodes
function_dict = {'devalue': devalue,
                 'addfield': addfield,
                 'fieldop': fieldop}

# Init ES class instance connecting to given ES nodes
es = Elasticsearch(node_list)
idx_list = _getinfo('idx_name')
if not idx_list:  # Quit program if no index was found
    print 'No index has been spotted in current cluster.'
    exit()

# Deal with different actions with if/elif/else
if args.action == 'mergeidx':
    idx_pattern = raw_input('pattern of src indices to be merged: ')
    dst_idx = raw_input('Name of the destination index to merge into: ')
    src_idx_list = list()
    for idx in idx_list:
        match = re.findall(idx_pattern, idx)
        if match or idx_pattern == idx:
            src_idx_list.append(idx)
            continue
    print 'List of source indices:'
    print src_idx_list
    flag_confirm = raw_input('Confirm?yes/no  ')
    if flag_confirm == 'no':
        print 'Terminate'
        exit()
    elif flag_confirm == 'yes':
        pass
    else:
        print 'Answer with yes or no'
    for src_idx in src_idx_list:
        reindex(src_idx=src_idx, dst_idx=dst_idx)
    flag_del_confirm = raw_input('Delete src indices?yes/no  ')
    if flag_del_confirm == 'yes':
        for src_idx in src_idx_list:
            es.indices.delete(index=src_idx)
elif args.action == 'puttemp':
    tmpl_list = es.indices.get_template()
    print "Existing templates:"
    for item in tmpl_list:
        print item
    print ''
    while True:
        tmpl_id = raw_input('Template ID: ')
        tmpl_file_path = raw_input('Template file full path: ')
        flag_confirm = raw_input('Confirm?yes/no  ')
        if flag_confirm == 'yes':
            break
        else:
            if flag_confirm != 'no':
                print 'Answer with yes or no'
            continue
    if tmpl_id in tmpl_list:
        print 'Template ID exists, will overwrite'
    else:
        print 'New template. Will create with this template id'
    flag_confirm = raw_input('Confirm?yes/no  ')
    if flag_confirm != 'yes':
        print 'Terminate'
        exit()
    renew_idx_template(tmpl_id, tmpl_file_path)
else:  # Left anything else to process HERE
    # Below codes will iterate across all indices matching preset pattern
    match_idx_list = list()
    ref_idx_list = list()
    idx_pattern = 'farm_op'
    ref_idx_pattern = 'tractor-*'
    try:
        for idx in idx_list:
            if fnmatch(idx, idx_pattern) or idx_pattern == idx:
                match_idx_list.append(idx)
                continue
            if fnmatch(idx, ref_idx_pattern) or ref_idx_pattern == idx:
                ref_idx_list.append(idx)
        if not match_idx_list:
            print 'No matching index for index pattern ' + idx_pattern
            exit()
    except:
        print 'Unable to get the list of existing indices, quitting'
        exit()
    # Execute operation specified in argument
    src_field = raw_input('Name of the field on which to perform operations\n')
    dst_field = raw_input('Name of the field to write into, put it in void if not necessary\n')
    doc_type = raw_input('Doc type:  ')
    for idx in match_idx_list:
        function_dict[args.action](index=idx, src_field=src_field, dst_field=dst_field, doc_type=doc_type,
                                   ref_idx_list=ref_idx_list)
