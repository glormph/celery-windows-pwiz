import sqlite3

"""SQLite atm, later use Postgres w Kantele"""


# The following string is a note on how to build the SQLite DB. No need to have
# a specific building function, this can be done by hand
"""
# Future tables will include
# dataset (kantele), dataset_raw (kantele), raw_mzml (make new)


CREATE TABLE mzmlfiles(id INTEGER PRIMARY KEY, fn TEXT, md5 TEXT);
CREATE TABLE mzml_galaxy(mzmlfile_id, galaxy_id TEXT);
                         FOREIGN KEY(file_id) REFERENCES mzmlfiles(id);

# can we have this table with datasets? conversion wf
# is not in kantele, so prob not yet
CREATE TABLE mzml_dataset(mzmlfile_id, dataset_id TEXT);
                         FOREIGN KEY(file_id) REFERENCES mzmlfiles(id);

CREATE TABLE analyses(id INTEGER PRIMARY KEY, name TEXT, date DATETIME,
                      state TEXT);
CREATE TABLE analysis_files(analysis_id INTEGER, file_id INTEGER,
                            FOREIGN KEY(analysis_id) REFERENCES analyses(id),
                            FOREIGN KEY(file_id) REFERENCES mzmlfiles(id);

# inputstore contents will be JSON in postgres, table analysis_input
# this will be used to fetch stuff from previous runs
CREATE TABLE analysis_input(id INTEGER PRIMARY KEY, analysis_id INTEGER,
                            contents TEXT);

"""

# example workflows:
# ----------
# start script show analyses, pick old one for rerun on new WF, recycle
# spectrafiles, recycle required other things

# CONVERT:
# raw file names stored in SQL db (or not when we do not have it yet), so their
# IDs will be passed
# converter converts
# scp and md5, return it in dict, check md5 after copying
#     retry inifinte/deadletter if necessary
# db task to store md5/mzml fn, retry infinite?
# ftp file to galaxy, retry infinite?
# import file to galaxy,  retry inifinite?
# store galaxy ID in DB


# QUEUE AN ANALYSIS
# CLI gets sourcehists, files in it are checked against DB
# OR, kantele dataset gets file galaxy_IDs
# probably good idea to have separate category on kantele for downloaded raws
# make collection, run wf, etc, update inputstore after each WFmod
# download

# RERUN
# select old analysis from db, and wf modules to rerun
# problem that old modules will not be same as new ones anymore and then you
# have a problem. Solution, do not delete old modules too quickly.
# new history is made
# preceding workflow outputs-datasets are resolved from old history and put
# into a new inputstore
# OR, datasets are gotten from old history with inputstore
# however, inputstore may contain errored datasets, or datasets that are
# waiting, or deleted since.
# Solution: use old inputstore, check for each dataset if they are 'ok' and
# populated and all elements are 'ok'
# If dsets are error, deleted etc, try to re-resolve in old history
# If that does not work, abort


def store_analysis(name):
    """Returns analysis id in DB"""
    pass


def get_analysis():
    pass


def update_inputstore(db, inputstore):
    """Run each time new datasets are discovered"""
    pass


def get_inputstore_json(db, analysis_id):
    pass


def get_analyses(db):
    sql = 'SELECT * FROM analyses'
    return db.execute(sql).fetch()


def get_connection(db):
    return sqlite3.Connection(db)


def run_fetch_sql(con, sql, values):
    cur = con.cursor()
    return cur.execute(sql, values)


def run_store_sql(con, sql, values):
    cur = con.cursor()
    cur.execute(sql, values)
    cur.commit()


def store_mzml_md5(con, fn, md5):
    sql = 'INSERT INTO mzmlfiles(fn, md5) VALUES(?, ?)'
    run_store_sql(con, sql, (fn, md5))


def store_mzml_galaxy(con, mzml_id, galaxy_id):
    sql = 'INSERT INTO mzml_galaxy(mzmlfile_id, galaxy_id) VALUES(?, ?)'
    run_store_sql(con, sql, (mzml_id, galaxy_id))


def get_mzml_id(con, fn, md5):
    sql = 'SELECT id FROM mzmlfiles WHERE fn=? AND md5=?'
    cur = run_fetch_sql(con, sql, (fn, md5))
    return cur.fetchone()[0]


def get_galaxy_mzml(con, mzml_id):
    sql = 'SELECT galaxy_id FROM mzml_galaxy WHERE mzmlfile_id=?'
    cur = run_fetch_sql(con, sql, (mzml_id,))
    return cur.fetchone()[0]
