workflows = [
    {
        'name': 'tmt10, ENSEMBL, QE', 'modules': [
            'quant iso', 'msgf tmt qe', 'percolator',
            'psm preproc isobaric ensembl', 'psm proteingroup',
            'psm post isobaric', 'peptide protein isobaric', 'gene isobaric',
            'protein isobaric', 'symbol isobaric'],
        'searchtype': 'standard',
        'not_outputs': [],
    },
    {
        'name': 'tmt10, uni/swissprot, QE', 'modules': [
            'quant iso', 'msgf tmt qe', 'percolator',
            'psm preproc isobaric uniswiss', 'psm proteingroup',
            'psm post isobaric', 'peptide protein isobaric', 'gene isobaric',
            'protein isobaric'],
        'searchtype': 'standard',
        'not_outputs': ['symbol table'],
    },
    {
        'name': 'varDB, tmt10, ENSEMBL, QE', 'modules': [
            'msgf tmt qe', 'percolator vardb', 'psm preproc isobaric vardb',
            'psm post isobaric vardb', 'peptide noncentric isobaric'],
        'searchtype': 'vardb',
        'rerun_rename_labels': {
            'quant lookup': False, 'expdata': False,
            'PSM table target': 'PSM table target normalsearch'}
        'not_outputs': ['protein table', 'gene table', 'symbol table'],
    },
        {'name': 'labelfree ENSEMBL QE', 'modules':
         ['quant labelfree', 'msgf labelfree qe', 'percolator',
          'psm preproc lfree ensembl', 'psm proteingroup',
          'psm post labelfree', 'peptide protein labelfree', 'gene labelfree',
          'protein labelfree', 'symbol labelfree'],
        'searchtype': 'standard'
        'not_outputs': [],
        }
]


wf_modules = {
    'quant iso': 'aaa52580-8f49-47c6-82d4-f217d513caf7',
    'quant labelfree': '6a91e9ab-731b-4617-b356-ae36790be4a0',
    'msgf tmt qe': '702f1f3a-f12f-4d11-8af0-f4e8d689fbce',
    'msgf labelfree qe': 'c6ca26bb-5dfb-4519-a081-d6de451468c4',
    'percolator': 'ba17a0df-74fb-45ed-8c1a-3f93e41797e3',
    'percolator vardb': 'a9dff6d4-abb3-44a8-8a4e-d7ac06bd4f47',
    'psm preproc isobaric ensembl': 'b914447e-bb66-480c-a399-345f19ec2d7d',
    'psm preproc isobaric uniswiss': '1d345cd7-b508-48d4-b747-6c6e78021324',
    'psm preproc lfree ensembl': '119c5de5-6048-41a0-8b63-5e489b435bf3',
    'psm preproc isobaric vardb': 'ae163ad8-1fbd-4d52-bf79-c652c7195da0',
    'psm preproc labelfree vardb': '13ff580a-244e-4863-836b-32182fccd1d1',
    'psm proteingroup': 'eb2e5ffe-9032-4ca7-b018-53ee9cff5707',
    'psm post isobaric': 'ab8506d7-dbf5-4d02-8c70-f34f6c1ef01a',
    'psm post isobaric vardb': '36a787a4-245c-4dc7-a4ad-52aa9b5c6fac',
    'psm post labelfree': '5b4333d8-169c-4242-b21e-2edff18a67c7',
    'peptide protein isobaric': 'a84dbd7e-d83e-4058-8151-70a24a97c3e1',
    'peptide noncentric isobaric': 'bb0d1638-b9dc-4e5d-895f-3cc46fa47b7d',
    'peptide protein labelfree': 'a84dbd7e-d83e-4058-8151-70a24a97c3e1',
    'peptide noncentric labelfree': '13ff580a-244e-4863-836b-32182fccd1d1',
    'protein isobaric': 'a420f771-1256-4cb4-92c1-67d3aaa3bd3e',
    'gene isobaric': '70d2a0b2-5bdc-4697-920a-c2f4d397249d',
    'symbol isobaric': 'ef400f26-ff63-47c4-8c98-b114a2e20618',
    'protein labelfree': 'a420f771-1256-4cb4-92c1-67d3aaa3bd3e',
    'gene labelfree': '70d2a0b2-5bdc-4697-920a-c2f4d397249d',
    'symbol labelfree': 'ef400f26-ff63-47c4-8c98-b114a2e20618',
}


download_data_names = {'pout2mzid target tar': 'mzidentml.tar.gz',
                       'PSM table target': 'psm_table.txt',
                       'peptide table': 'peptide_table.txt',
                       'protein table': 'protein_table.txt',
                       'gene table': 'gene_table.txt',
                       'symbol table': 'symbol_table.txt',
                       }

collection_names = ['spectra',
                    'msgf target',
                    'msgf decoy',
                    'msgf tsv target',
                    'msgf tsv decoy',
                    'pout2mzid target',
                    'pout2mzid decoy',
                    'perco tsv target',
                    'perco tsv decoy',
                    'postprocessed PSMs target',
                    'postprocessed PSMs decoy',
                    'peptides bare target',
                    'peptides bare decoy',
                    ]

flatfile_names = ['target db',
                  'decoy db',
                  'knownpep db',
                  'biomart map',
                  'spectra lookup',
                  'modifications',
                  'expdata',
                  'quant lookup',
                  'PSM lookup target',
                  'PSM lookup decoy',
                  'preprocessed PSMs target',
                  'preprocessed PSMs decoy',
                  'pout2mzid target tar',  # output
                  'PSM table target',  # output
                  'PSM table decoy',
                  'proteingroup lookup target',
                  'proteingroup lookup decoy',
                  'peptide table',  # output
                  'protein table',  # output
                  'gene table',  # output
                  'symbol table',  # output
                  ]


libraries = {'modifications': '85572375f4abb7f8',
             'databases': '2eb7685dc8c8283c',
             'marts': '2eb7685dc8c8283c',  # same as databases
            }
