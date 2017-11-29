workflows = [
    # FIXME runtime params cannot be checked before running,
    # because not-connected datasets
    # are also runtime vars and cannot be distinguished. Therefore, be very
    # careful and do a dry run so you will see WARNINGs when not filling in
    # params (and meaningless warnings for eg martmap in varDB search)
    {
        'name': 'ENSEMBL', 'modules': [
            ('base_search', '0.1', 'proteingenessymbols'),
        ],
        'searchtype': 'standard',
        'dbtype': 'ensembl',
        'required_params': ['instrument', 'modifications',
                            'setpatterns', 'setnames',
                            #'fr_matcher', 'strips', 'strippatterns',
                            'perco_ids', 'ppoolsize'],
        'required_dsets': [],
        'his_inputs': [],
        'lib_inputs': ['target db', 'decoy db', 'biomart map',
                       'knownpep predpi tabular'],
        'other_inputs': ['strips'],
    },
    {
        'name': 'uniprot', 'modules': [
            ('base_search', '0.1', 'proteingenes'),
        ],
        'searchtype': 'standard',
        'dbtype': 'uniprot',
        'required_params': ['instrument', 'modifications',
                            'setpatterns', 'setnames',
                            #'fr_matcher', 'strips', 'strippatterns',
                            'perco_ids', 'ppoolsize'],
        'required_dsets': [],
        'his_inputs': [],
        'lib_inputs': ['target db', 'decoy db', 'knownpep predpi tabular'],
        'other_inputs': ['strips'],
    },
    {
        'name': 'proteincentric DB', 'modules': [
            ('base_search', '0.1', 'proteins'),
        ],
        'searchtype': 'standard',
        'dbtype': 'other',
        'required_params': ['instrument', 'modifications',
                            'setpatterns', 'setnames',
                            #'fr_matcher', 'strips', 'strippatterns',
                            'perco_ids', 'ppoolsize'],
        'required_dsets': [],
        'his_inputs': [],
        'lib_inputs': ['target db', 'decoy db',
                       'knownpep predpi tabular'],
        'other_inputs': ['strips'],
    },
    {
        'name': 'Peptidecentric', 'modules': [
            ('base_search', '0.1', 'peptides noncentric'),
        ],
        'searchtype': 'standard',
        'dbtype': 'other',
        'required_params': ['instrument', 'modifications',
                            'setpatterns', 'setnames',
                            #'fr_matcher', 'strips', 'strippatterns',
                            'perco_ids', 'ppoolsize'],
        'required_dsets': [],
        'his_inputs': [],
        'lib_inputs': ['target db', 'decoy db',
                       'knownpep predpi tabular'],
        'other_inputs': ['strips'],
    },
    {
        'name': 'Peptides unfiltered WXS', 'modules': [
            ('unfiltered_search', '0.1', 'peptides noncentric'),
        ],
        'searchtype': 'standard',
        'dbtype': 'other',
        'required_params': ['instrument', 'modifications',
                            'setpatterns', 'setnames',
                            #'fr_matcher', 'strips', 'strippatterns',
                            'perco_ids', 'ppoolsize'],
        'required_dsets': [],
        'his_inputs': [],
        'lib_inputs': ['target db', 'decoy db',
                       'knownpep predpi tabular'],
        'other_inputs': ['strips'],
    },
    {
        'name': 'varDB and knownpep concatenation', 'modules': [
            ('vardb_search', '0.1', 'peptides noncentric'),
        ],
        'searchtype': 'vardb',
        'dbtype': 'other',
        'required_params': ['instrument', 'modifications',
                            'setpatterns', 'setnames',
                            'fdrclasses', 'decoy_fdrclasses',
                            #'fr_matcher', 'strips', 'strippatterns',
                            'perco_ids', 'ppoolsize'],
        'required_dsets': ['psm table normalsearch',
                           'quant lookup'],
        'his_inputs': [],
        'lib_inputs': ['target db', 'decoy db',
                       'knownpep predpi tabular', 'knownpep db',
                       'knownpep db decoy',
                       'knownpep tryp lookup', 'knownpep allpep lookup'],
        'other_inputs': ['strips'],
    },
    {
        'name': 'IPG 6RF', 'modules': [
            ('6rf_search', '0.1', 'peptides noncentric'),
        ],
        'searchtype': '6rf',
        'dbtype': 'other',
        'required_params': ['instrument', 'modifications',
                            'setpatterns', 'setnames',
                            'fr_matcher', 'firstfrac',
                            'strips', 'strippatterns',
                            'perco_ids', 'ppoolsize'],
        'required_dsets': ['psm table normalsearch',
                           'quant lookup'],
        'his_inputs': [],
        'lib_inputs': ['knownpep predpi tabular', 'knownpep db',
                       'knownpep db decoy', 'pipeptides db',
                       'knownpep tryp lookup', 'knownpep allpep lookup'],
        'other_inputs': ['strips'],
    },
]


collection_names = ['spectra']

flatfile_names = ['target db',
                  'decoy db',
                  'knownpep predpi tabular',
                  'pipeptides db',
                  'pipeptides known db',
                  'knownpep db',
                  'knownpep db decoy',
                  'knownpep tryp lookup',
                  'knownpep allpep lookup',
                  'biomart map',
                  'spectra lookup',
                  'quant lookup',
                  'psm lookup target',
                  'psm lookup decoy',
                  'psm table target',
                  'psm table decoy',
                  'psm table normalsearch',
                  ]


# Montezuma
libraries = {'databases': 'a3437b7b0c19ebd1',
             'marts': '86ed203ff9a94938',
             'pipeptides': '128140af13bad45b',

             #'knownpeptides': 'e9b47af80022b6ee',
             # use same as databases
             'knownpeptides': 'a3437b7b0c19ebd1',

             'lookups': 'b981d689ab1ff53a',
             }


strips = {'37-49': {'fr_width': 0.0174,
                    'pi_tolerance': 0.08,
                    'intercept': 3.5959,
                    'fr_amount': 72,
                    'reverse': False},
          '3-10': {'fr_width': 0.0676,
                   'pi_tolerance': 0.11,
                   'intercept': 3.5478,
                   'fr_amount': 72,
                   'reverse': False},
          '11-6': {'fr_width': -0.0762,
                   'pi_tolerance': 0.11,
                   'intercept': 10.3936,
                   'fr_amount': 60,
                   'reverse': True},
          '6-9': {'fr_width': 0.0336,
                  'pi_tolerance': 0.11,
                  'intercept': 6.1159,
                  'fr_amount': 72,
                  'reverse': False},
          }


uploadable_files = [('target db', 'fasta'),
                    ('decoy db', 'fasta'),
                    ('psm table normalsearch', 'tabular'),
                    ('quant lookup', 'sqlite'),
                    ]

strips = {name: {**strips[name], 'name': name} for name in strips}
