import configparser
import sys


def development(config):
    config.set('run', 'server_add', 'https://k8s.alia-space.com/bedev')
    config.set('postgresql', 'host', 'nutsdbdev-pgbouncer.nuts-dev.svc')
    config.set('prefect', 'project_name', 'NUTS-DEV')
    config.set('prefect', 's2_WF_prefix', 'S2_DEV')
    config.set('prefect', 's2Est_WF_prefix', 'S2EST_DEV')
    config.set('prefect', 'dem_WF_prefix', 'DEM_DEV')
    config.set('prefect', 'demEst_WF_prefix', 'DEMEST_DEV')


def production(config):
    config.set('run', 'server_add', 'https://k8s.alia-space.com/be')
    config.set('postgresql', 'host', 'nutsdb-pgbouncer.nuts.svc')
    config.set('prefect', 'project_name', 'NUTS')
    config.set('prefect', 's2_WF_prefix', 'S2')
    config.set('prefect', 's2Est_WF_prefix', 'S2EST')
    config.set('prefect', 'dem_WF_prefix', 'DEM')
    config.set('prefect', 'demEst_WF_prefix', 'DEMEST')


def default(config):
    # Add the machine specs
    config.add_section('run')
    config.set('run', 'machine', 'cluster')  # local   'cluster'
    config.set('run', 'repo_url', 'aliaspace')

    # Add the machine specs
    config.add_section('prefect')

    # Add the db structure
    config.add_section('postgresql')
    config.set('postgresql', 'port', '5432')
    config.set('postgresql', 'user', 'XXXXXXXXXX')  # nuts_user  nutsuser
    config.set('postgresql', 'password', 'XXXXXXXXXX')
    config.set('postgresql', 'database', 'nuts_db')

    # Add the geoserver structure
    config.add_section('geoserver')
    config.set('geoserver', 'ows_host', 'https://geoserver.alia-space.com/geoserver/ows')
    config.set('geoserver', 'wms_host', 'https://geoserver.alia-space.com/geoserver/gds/wms')
    config.set('geoserver', 'wcs_host', 'https://geoserver.alia-space.com/geoserver/gds/wcs')

    # Add the Copernicus hub structure
    config.add_section('s2hub')
    config.set('s2hub', 'host', 'https://scihub.copernicus.eu/dhus')
    config.set('s2hub', 'user', 'XXXXXXXXXX')
    config.set('s2hub', 'password', 'XXXXXXXXXX')
    config.set('s2hub', 'csv_tables', 'https://scihub.copernicus.eu/catalogueview')

    # Add storing folders structure
    config.add_section('res_folders')
    config.set('res_folders', 'cache_folder', '/usr/share/cache_folder')
    config.set('res_folders', 'result_folder', '/usr/share/result_folder')
    #config.set('res_folders', 'results_url',
    #           'https://k8s.alia-space.com/files/file-explorer?dir=/opt/job_folder/products_ready')
    config.set('res_folders', 'results_url',
               'https://k8s.alia-space.com/files-dev/files')
    config.set('res_folders', 'res_user', 'XXXXXXXXXX')
    config.set('res_folders', 'res_psw', 'XXXXXXXXXX')

    # Add the email notifier structure
    config.add_section('notifier')
    config.set('notifier', 'email_sender', 'XXXXXXXXXX')
    config.set('notifier', 'credentials', '/opt/nuts/nuts-gmail.pickle')


if __name__ == '__main__':

    if len(sys.argv) < 2:
        raise ValueError('No environment defined')
    elif len(sys.argv) > 2:
        raise ValueError('To many parameters')
    else:
        env = sys.argv[1]

    config = configparser.ConfigParser()

    if env == 'dev':
        default(config)
        development(config)
        # Write the structure to ini file
        with open(r"configfile.ini", "w") as configfile:
            config.write(configfile)
    elif env == 'prod':
        default(config)
        production(config)
        # Write the structure to ini file
        with open(r"configfile.ini", "w") as configfile:
            config.write(configfile)
    else:
        raise ValueError('Invalid environment name')
