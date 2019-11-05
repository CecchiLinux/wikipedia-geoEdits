#!/usr/bin/python

import sys
import re

def filter_print_record(revision_fields, vals):
    (article_id, rev_id, article_title, 
             timestamp, username, user_id) = revision_fields

    ## if you want to exclude a given variable,
    ## just put a '#' character in front of the
    ## relevant line to comment it out
    if (minor_filter and vals['MINOR'] == '1'): # filter minors
        return 
    if (authenticated_filter and not pattern.match(user_id)): # filter authenticated users and ips that needs DNS (~58.000)
        return 
    if not vals['CATEGORY'].rstrip():
        return

    #print('\t'.join([
    print('|'.join([
        article_id,
        rev_id,
        article_title,
        #timestamp,
        #username,
        user_id,
        vals['CATEGORY'],
        #vals['IMAGE'],
        #vals['MAIN'],
        #vals['TALK'],
        #vals['USER'],
        #vals['USER_TALK'],
        #vals['OTHER'],
        #vals['EXTERNAL'],
        #vals['TEMPLATE'],
        #vals['COMMENT'],
        #vals['MINOR'],
        vals['TEXTDATA']
    ]))


minor_filter = True 
authenticated_filter = True
pattern = re.compile("^(ip:)\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$") # regular expression for ips

vals = dict()

for line in sys.stdin:
    fields = line.rstrip().split(' ')
    field_name = fields[0]
    if field_name == "":
        if len(vals) > 0:
            filter_print_record(revision_fields, vals)
            vals.clear() # dictionary clean
    elif field_name == "REVISION":
        revision_fields = fields[1:]
    else:
        vals[field_name] = ' '.join(fields[1:])

## output last record
if len(vals) > 0:
    filter_print_record(revision_fields, vals)

