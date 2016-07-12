from bioblend import galaxy

from tasks import config


def get_galaxy_instance(inputstore):
    return galaxy.GalaxyInstance(config.GALAXY_URL,
                                 key=config.APIKEYS[inputstore['user']])
