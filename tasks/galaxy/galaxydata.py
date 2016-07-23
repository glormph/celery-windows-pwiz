workflows = [
    {
        'name': 'tmt10, ENSEMBL, QE', 'modules': [
            'quant iso', 'msgf tmt qe', 'percolator',
            'psm preproc isobaric ensembl', 'psm proteingroup',
            'psm post isobaric', 'peptide protein isobaric', 'gene isobaric',
            'protein isobaric', 'symbol isobaric'],
    },
    {
        'name': 'varDB, tmt10, ENSEMBL, QE', 'modules': [
            'msgf tmt qe', 'percolator vardb', 'psm preproc isobaric vardb',
            'psm post isobaric vardb', 'peptide noncentric isobaric'],
        'rerun_rename_labels': {
            'quant lookup': False, 'expdata': False,
            'PSM table target': 'PSM table target normalsearch'}
    },
#        {'name': 'labelfree ENSEMBL QE', 'modules':
#         ['quant labelfree', 'msgf labelfree qe', 'percolator',
#          'psm preproc lfree ensembl', 'psm proteingroup',
#          'psm post labelfree', 'peptide protein labelfree', 'gene labelfree',
#          'protein labelfree', 'symbol labelfree']},
]

# For varDB, need to either run a conventional search at the same time,
# or be passed a history with search data. What is needed specifically is:
# - PSM table postprocessed for median norm
# - quant lookup for not having to rerun the quant stuff
# When history w-normal search is passed, dont run the normal search,
# else chain both?
# We could run in same history but then dataset labels will be fucked up
# Could also use this in a rerun, but not really sure how to spec rerun types


wf_modules = {
    'quant iso': 'aaa52580-8f49-47c6-82d4-f217d513caf7',
    #'quant labelfree': '6a91e9ab-731b-4617-b356-ae36790be4a0',
    'msgf tmt qe': '702f1f3a-f12f-4d11-8af0-f4e8d689fbce',
    #'msgf labelfree qe': '515ddc0b-67ee-44fd-9834-c4b98608cd96',
    'percolator': '259ca0a4-0c2f-49f9-a4bf-2c41634bf454',
    'percolator vardb': 'a9dff6d4-abb3-44a8-8a4e-d7ac06bd4f47',
    'psm preproc isobaric ensembl': 'b914447e-bb66-480c-a399-345f19ec2d7d',
    #'psm preproc lfree ensembl': '9548e8a3-7549-4c5a-9e9f-42b68a7a9737',
    'psm preproc isobaric vardb': '52f0f121-159c-42b4-a9d2-3b869ff86afb',
    #'psm preproc labelfree vardb': '394e248a-c987-47be-8738-b5aed7a71d9d',
    'psm proteingroup': 'eb2e5ffe-9032-4ca7-b018-53ee9cff5707',
    'psm post isobaric': '6815089a-1e7b-4b55-a0a4-5b6ce86d7d92',
    'psm post isobaric vardb': 'cda5fa72-3dc1-427d-8fa9-26c87b0e2ad3',
    'psm post labelfree': '5e6a9679-7917-499e-9308-b79bbb486361',
    'peptide protein isobaric': 'a84dbd7e-d83e-4058-8151-70a24a97c3e1',
    'peptide noncentric isobaric': 'bb0d1638-b9dc-4e5d-895f-3cc46fa47b7d',
    'protein isobaric': 'a420f771-1256-4cb4-92c1-67d3aaa3bd3e',
    'gene isobaric': '70d2a0b2-5bdc-4697-920a-c2f4d397249d',
    'symbol isobaric': 'ef400f26-ff63-47c4-8c98-b114a2e20618',
}


download_data_names = {'pout2mzid target': 'mzidentml.tar.gz',
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
                    'pout2mzid target',  # output
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
                  'PSM table target',  # output
                  'PSM table decoy',
                  'proteingroup lookup target',
                  'proteingroup lookup decoy',
                  'peptide table',  # output
                  'protein table',  # output
                  'gene table',  # output
                  'symbol table',  # output
                  ]
