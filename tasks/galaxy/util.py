from bioblend import galaxy


def get_galaxy_instance(inputstore):
    return galaxy.GalaxyInstance(inputstore['galaxy_url'],
                                 inputstore['apikey'])
