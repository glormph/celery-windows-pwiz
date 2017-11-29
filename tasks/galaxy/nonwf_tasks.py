from tasks.galaxy import tasks as runtasks


tasks = {'@create_6rf_split_dbs': {'task': runtasks.create_6rf_split_dbs,
                                     'inputs': ['peptable MS1 deltapi'],
                                     'params': ['fr_widthlist', 'interceptlist',
                                                'fr_amounts', 'pi_tolerances',
                                                'reverses',
                                                'setpatterns', 'setnames',
                                                'strips', 'strippatternlist'],
                                     # outputs have dynamic names FIXME make dict of it
                                     # maybe [name][td] = {'src': 'hdca', 'id': 1234}
                                     'outputs': [''],
                                     },
         '@create_spectra_db_pairedlist': {
                                  'task': runtasks.create_spectra_db_pairedlist,
                                  # FIXME there are more inputs but they are
                                  # dynamically named after the set/pi
                                  'inputs': ['spectra'],
                                  'params': ['code',
                                             'setpatterns',
                                             'strips', 'strippatternlist'],
                                  'outputs': ['spectra target db',
                                              'spectra decoy db'],
                                     },
         }


# FIXME
# put in collection- sourcehis is now in datasets/inputstore, other_names, fix workflow parser/starter/dset init
