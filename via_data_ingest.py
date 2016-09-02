#!/usr/bin/env python

import re
import pycurl
from StringIO import StringIO
import json
from pprint import pprint
import MySQLdb

db = MySQLdb.connect("","","","")
cursor = db.cursor()

# Purge tblPhotographs before repopulating
try:
    sql = "truncate table tblPhotographs"
    cursor.execute(sql)
except:
    print "Error: unable to truncate table"

num_found = 1
pagination_start_curr = 0
pagination_size = 10

queries = [
"http://api.lib.harvard.edu/v2/items.json?q=Military+Tribunal+Case+Three+photograph+collection",
"http://api.lib.harvard.edu/v2/items.json?q=nuremberg+trial+photograph+collection",
"http://api.lib.harvard.edu/v2/items.json?q=nuremberg+trial+photograph+collection&physicalLocation=Harvard+Law+School+Library"
]

for query in queries:
    num_found = 1
    pagination_start_curr = 0
    pagination_size = 10
    while (num_found >= pagination_start_curr):
        api_url = "{0}&start={1}&limit={2}".format(query, pagination_start_curr, pagination_size)
        print "\n###########################\n"
        print "api_url: {0}\n".format(api_url)
    
        buffer = StringIO()
        c = pycurl.Curl()
        c.setopt(c.URL, api_url)
        c.setopt(c.WRITEFUNCTION, buffer.write)
        c.perform()
        c.close()
        
        body = buffer.getvalue()
        data = json.loads(body)
        num_found = data['pagination']['numFound']
        pagination_start_prev = data['pagination']['start']
        print "num_found: {0}".format(num_found)
        print "pagination_start_prev: {0}".format(pagination_start_prev)
        pagination_start_curr = pagination_start_prev + pagination_size
        print "pagination_start_curr: {0}".format(pagination_start_curr)
        for mods_record in data['items']['mods']:
            # Populate: inscription, local_system_id, full_image_url, thumbnail_url
            inscription = None
            local_system_id = None
            full_image_url = None
            thumbnail_url = None
            date = None
            abstract = None
            print "\n##########\n"
            print "dump mods_record['relatedItem']: "
            pprint(mods_record)
            try:
                record_id = mods_record['relatedItem']['recordInfo']['recordIdentifier']
                if record_id:
                    print "primary-pattern record_id: " + record_id
                    local_system_id = record_id
            except:
                print "no primary-pattern record_id found"
                try:
                    record_id = mods_record['recordInfo']['recordIdentifier']['#text']
                    if record_id:
                        print "secondary-pattern record_id: " + record_id
                        local_system_id = record_id
                except:
                    print "no secondary-pattern record_id found"
                    try:
                        record_id_alt = mods_record['identifier']['#text']
                        if record_id_alt:
                            print "record_id_alt: " + record_id_alt
                            local_system_id = record_id_alt
                    except:
                        print "no alternate record_id found"
            try:
                abstract = mods_record['relatedItem']['abstract']
                #if abstract:
                    #print "abstract: " + abstract
            except:
                print "no abstract found"
            try:
                note = mods_record['relatedItem']['note']
                if note:
                    for line in note:
                        subject_match = re.search(r'^Subject:\s*(.*)$', line)
                        if subject_match:
                            subject_clean = subject_match.group(1)
                            print "primary-pattern note subject: " + subject_clean
                    for line in note:
                        inscription_match = re.search(r'^Inscription:\s*(.*)$', line)
                        if inscription_match:
                            inscription_clean = inscription_match.group(1)
                            print "primary-pattern note inscription: " + inscription_clean
                            inscription = inscription_clean
            except:
                print "no primary-pattern note found"
                try:
                    note = mods_record['note']
                    if note:
                        for line in note:
                            subject_match = re.search(r'^Subject:\s*(.*)$', line)
                            if subject_match:
                                subject_clean = subject_match.group(1)
                                print "secondary-pattern note subject: " + subject_clean
                        for line in note:
                            inscription_match = re.search(r'^Inscription:\s*(.*)$', line)
                            if inscription_match:
                                inscription_clean = inscription_match.group(1)
                                print "secondary-pattern note inscription: " + inscription_clean
                                inscription = inscription_clean
                except:
                    print "no secondary-pattern note found"
            try:
                for subject in mods_record['subject']:
                    topic = subject['topic']
                    if topic:
                        print "topic: " + topic
            except:
                print "no subject found"
            try:
                for url in mods_record['relatedItem']['relatedItem']['location']['url']:
                    if url['@displayLabel'] == 'Full Image':
                        print "primary-pattern full image url: " + url['#text']
                        full_image_url = url['#text']
                    elif url['@displayLabel'] == 'Thumbnail':
                        print "primary-pattern thumbnail url: " + url['#text']
                        thumbnail_url = url['#text']
            except:
                print "no primary-pattern url found"
                try:
                    for url in mods_record['relatedItem']['location']['url']:
                        if url['@displayLabel'] == 'Full Image':
                            print "secondary-pattern full image url: " + url['#text']
                            full_image_url = url['#text']
                        elif url['@displayLabel'] == 'Thumbnail':
                            print "secondary-pattern thumbnail url: " + url['#text']
                            thumbnail_url = url['#text']
                except:
                    print "no secondary-pattern url found"
            try:
                date = mods_record['originInfo']['dateCreated'][2]
                if date:
                    print "date: " + date
            except:
                print "no date found"
            print "verify full_image_url: {0}".format(full_image_url)
            
            try:
                print "primary-pattern inscription: " + inscription
            except:
                try:
                    abstract = mods_record["relatedItem"]["abstract"]
                    inscription = abstract.encode('utf-8')
                    print "secondary-pattern inscription: " + abstract
                except:
                    print "secondary-pattern inscription failed"
                    try:
                        abstract = mods_record["abstract"]
                        inscription = abstract.encode('utf-8')
                        print "tertiary-pattern inscription: " + abstract
                    except:
                        print "tertiary-pattern inscription failed"
            sql = "insert into tblPhotographs (Inscription, Date, LocalSystemID, FullImageURL, ThumbnailURL, MaterialType) values (%s, %s, %s, %s, %s, %s)"
            try:
               # Execute the SQL command
                cursor.execute(sql, (inscription, date, local_system_id, full_image_url, thumbnail_url, 'photograph'))
            except MySQLdb.Error, e:
                try:
                    print "MySQL Error [%d]: %s" % (e.args[0], e.args[1])
                except IndexError:
                    print "MySQL Error: %s" % str(e)
                print "Error: unable to insert data"

db.close()
