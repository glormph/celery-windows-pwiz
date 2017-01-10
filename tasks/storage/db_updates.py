from celeryapp import app
from tasks import config
import dbaccess


@app.task(queue=config.DBUPDATE, bind=True)
def update_mzml_md5(self, inputstore):
    con = dbaccess.get_connection(inputstore['dbfile'])
    dbaccess.store_mzml_md5(con, inputstore['mzml'], inputstore['mzml_md5'])
    inputstore['mzml_dbid'] = dbaccess.get_mzml_id(con, inputstore['mzml'],
                                                   inputstore['mzml_md5'])
    con.close()
    return inputstore


@app.task(queue=config.DBUPDATE, bind=True)
def update_mzml_galaxy(self, inputstore):
    con = dbaccess.get_connection(inputstore['dbfile'])
    dbaccess.store_mzml_galaxy(con, inputstore['mzml_dbid'],
                               inputstore['galaxy_dset'])
    inputstore['galaxy_id'] = dbaccess.get_galaxy_mzml(con,
                                                       inputstore['mzml_dbid'])
    con.close()
    return inputstore
