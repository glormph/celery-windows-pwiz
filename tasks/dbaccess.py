# files: id, fn, md5, createdate, type (raw/mzml) # UI CENTERING
# storage: id, files_id, path, server
# galaxyfiles: id, files_id, dset_id, dset_name
# collections: id, galaxy_fn_id, coll_id, search_id
# search: id, title, wf_id, ### wf_version  ## UI CENTERING
# searchfiles: id, search_id, files_id
# results: id, search_id, json_summary
# resultfiles: id, files_id, search_id
# workflows: ???

# for testing, plz fix foreign keying for the real deal:
# galaxyfiles: files_id
# collections: galaxy_fn_id, search_id
# search: wf_id
# results search_id
# resultfiles: files_id, search_id
"""
CREATE TABLE files(id INTEGER PRIMARY KEY, fn VARCHAR,
                   md5 VARCHAR, date TIMESTAMP, type VARCHAR);
CREATE TABLE storage(id INTEGER PRIMARY KEY, files_id INTEGER, path VARCHAR,
server VARCHAR);
CREATE TABLE galaxyfiles(id INTEGER PRIMARY KEY,
files_id INTEGER, dset_id VARCHAR, dset_name VARCHAR);
CREATE TABLE collections(id INTEGER PRIMARY KEY,
galaxy_fn_id INTEGER, coll_id VARCHAR, search_id INTEGER);
CREATE TABLE search(id INTEGER PRIMARY KEY, title VARCHAR,
wf_id INTEGER);
CREATE TABLE searchfiles(id INTEGER PRIMARY KEY,
search_id INTEGER, files_id INTEGER);
CREATE TABLE results(id INTEGER PRIMARY KEY, search_id INTEGER,
json_summary JSON);
CREATE TABLE resultfiles(id INTEGER PRIMARY KEY,
files_id INTEGER, search_id INTEGER);
CREATE TABLE workflows(id INTEGER PRIMARY KEY, name VARCHAR);
"""


#import psycopg2
from sqlite3 import Connection

#from tasks import config

#conn = psycopg2.connect(config.DB_URI)
conn = Connection('test-proteomics-tasks.sqlite')


def lookup(sql, fields=False):
    cur = conn.cursor()
    if fields:
        cur.execute(sql, fields)
    else:
        cur.execute(sql)
    return cur.fetchall()


def editmany(sql, values):
    cur = conn.cursor()
    cur.executemany(sql, values)
    cur.commit()


def edit(sql, values):
    cur = conn.cursor()
    cur.execute(sql, values)
    cur.commit()


def upload_file(file_id, dset_id, dset_name):
    edit('INSERT INTO galaxyfiles VALUES(?, ?, ?)',
         (file_id, dset_id, dset_name))


def store_collection(search_id, collection_id, fn_ids):
    edit('INSERT INTO collections VALUES(?, ?, ?)',
         ((collection_id, x, search_id) for x in fn_ids))


def init_search(title, wf_id, mzml_ids):
    edit('INSERT INTO search(title, wf_id) VALUES(?, ?)', (title, wf_id))
    search_id = lookup('SELECT id FROM search WHERE title=? AND wf_id=?')[0]
    editmany('INSERT INTO searchfiles VALUES(?, ?)',
             ((search_id, x) for x in mzml_ids))
    return search_id


def get_fullpath(fn_id, server):
    sql = ('SELECT st.path, f.fn FROM files AS f '
           'JOIN storage AS st ON f.id=st.files_id '
           'WHERE f.id=? AND st.server=?')
    return lookup(sql, (fn_id, server))


def get_name_id_hdas(search_id):
    sql = ('SELECT gf.dset_name, gf.dset_id FROM search AS s '
           'JOIN searchfiles AS sf ON s.id=sf.search_id '
           'JOIN files AS f ON sf.files_id=files.id '
           'JOIN galaxyfiles AS gf ON f.id=gf.files_id '
           'WHERE s.id=?')
    return lookup(sql, search_id)
