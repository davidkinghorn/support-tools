#!/opt/vsnap/venv/bin/python3

import sys
sys.path.append('/opt/vsnap/lib')
import click
import logging
import logging.handlers

from vsnap import config
from vsnap.common import const
from vsnap.linux import system

from vsnap.cloud import core as cloudcore
from vsnap.cloud import util as cloudutil
from vsnap.cloud import client as cloudclient

CURRENT_VERISON = system.get_package_version("vsnap")
VERSION_1016 = '10.1.6'
VERSION_1015 = '10.1.5'
VERSION_1014 = '10.1.4'

cloudmdl = None
archmdl = None
cloudcfg = None
archcfg = None

if VERSION_1016 in CURRENT_VERISON:
    from vsnap.cloud import model as cloudmdl
    from vsnap.archive import model as archmdl
    cloudcfg = cloudmdl.CloudConfig()
    archcfg = archmdl.ArchiveConfig()
elif VERSION_1015 in CURRENT_VERISON or VERSION_1014 in CURRENT_VERISON:
    from vsnap.cloud import config as cloudcfg
    from vsnap.archive import config as archcfg
    cloudmdl = cloudcfg
    archmdl = archcfg

@click.command(help="Import cloud/archive metadata.")
@click.option("--endpoint", prompt="Endpoint URL", help="Cloud endpoint URL")
@click.option("--api_key", prompt="Access Key", help="Access key for authentication")
@click.option("--api_secret", prompt="Secret Key", hide_input=True, help="Secret key for authentication")
@click.option("--bucket", prompt="Bucket", help="Bucket name")
@click.option("--provider", prompt="Provider Type", help="Cloud provider type: sp, cos, aws, azure, generic")
@click.option("--cert_file", help="Full path to SSL certificate file")
@click.option("--archive", required=False, is_flag=True, help="Cloud bucket is used for archive")
@click.option("--deep_storage", required=False, is_flag=True, help="Use deep storage (AWS only)")
@click.option("--is_global", required=False, is_flag=True, help="Import cloud/archive metadata for all vSnap servers")
def catalog(endpoint, api_key, api_secret, bucket, provider, cert_file, archive, deep_storage, is_global):
    click.echo("Adding %s partner" % ('archive' if archive else 'cloud'))
    partner_id = cloudcore.add_partner(endpoint, None, None, api_key, api_secret, True, bucket, provider, 16*1024*1024, [], cert_file, archive, deep_storage)
    part_db = None
    typecfg = None
    tag_model = None
    volume_model = None
    volume_options_model = None
    snapshot_model = None

    if archive:
        part_db = archcfg.get_partner_by_id(partner_id)
        typecfg = archcfg
        tag_model = archmdl.ArchiveTag
        volume_model = archmdl.ArchiveVolume
        volume_options_model = get_volume_options_model(archive)
        snapshot_model = archmdl.ArchiveSnapshot
    else:
        part_db = cloudcfg.get_partner_by_id(partner_id)
        typecfg = cloudcfg
        tag_model = cloudmdl.CloudTag
        volume_model = cloudmdl.CloudVolume
        volume_options_model = get_volume_options_model()
        snapshot_model = cloudmdl.CloudSnapshot
    
    click.echo("Preparing to scan bucket")
    cloudinfo = cloudutil.get_cloud_info(part_db)
    client = cloudclient.get_client(cloudinfo['endpoint'], bucket_name=cloudinfo['bucketname'], access_key=cloudinfo['accesskey'], secret_key=cloudinfo['secretkey'], 
        secure=cloudinfo["endpoint_secure"], provider=cloudinfo["provider"], is_archive=archive, deep_storage=deep_storage)
    catalog_consolidate(typecfg, tag_model, snapshot_model, volume_model, volume_options_model, client, is_global)


def catalog_consolidate(typecfg, tag_model, snapshot_model, volume_model, volume_options_model, client, is_global=False):
    click.echo("Scanning bucket for vSnap system IDs")
    local_owner_system_id = config.get_system_option_by_name(const.LOCAL_SYSTEM_ID).value
    owner_system_ids = []
    # if global check the names of every tag file to retrive a list of owner system ids: <owner_system_id>-uuid
    if is_global:
        tag_files_names = cloudutil.get_metadata_names(client, const.CLOUD_TAGS_DIR+'/')
        for tag_file in tag_files_names:
            owner_system_id = tag_file.split("-")[0].split("/")[-1]
            if owner_system_id not in owner_system_ids:
                owner_system_ids.append(owner_system_id)
    # else only retrieve metadata for this vsnap
    else:
        owner_system_ids.append(local_owner_system_id)

    # import tags metadata for each vsnap requested
    click.echo("Importing tag metadata from bucket")
    total = len(owner_system_ids)
    current = 1
    for owner_system_id in owner_system_ids:
        click.echo("Importing tag metadata for system ID %s of %s: %s" % (current, total, owner_system_id))
        current += 1
        tags_data = cloudutil.get_metadata_object(client, const.CLOUD_TAGS_DIR+'/'+owner_system_id+'-')
        if tags_data:
            conn = sqlite_connect(typecfg)
            tags_updated = cloudutil.Importer(**get_tags_import_args(conn, volume_model, tag_model)).restore(tags_data)['updated']
            if tags_updated > 0:
                click.echo("Restored %s tags for system ID %s" % (tags_updated, owner_system_id))
            sqlite_disconnect(conn)

    # iterate through volume directories
    click.echo("Scanning bucket for volume metadata")
    volumes_restored = 0
    snapshots_restored = 0
    metadata_dir = const.SPP_METADATA+'/'
    if client.is_archive:
        metadata_dir = const.LOCAL_METADATA+'/'
    volume_dir_names, _ = client.list_objects(prefix=metadata_dir, recursive=True)
    volume_ids = []
    for volume_dir in volume_dir_names:
        volume_id = volume_dir.replace(metadata_dir, '').split("/")[0]
        if volume_id == const.CLOUD_TAGS_DIR or volume_id == const.CLOUD_SPP_FILE or volume_id in volume_ids:
            continue
        if client.archive_lifecycle_required():
            if (not client.metadata_restored(volume_id+'/'+const.CLOUD_VOLUMES_PREFIX) or 
               not client.metadata_restored(volume_id+'/'+const.CLOUD_VOLUME_OPTIONS_PREFIX) or
               not client.metadata_restored(volume_id+'/'+const.CLOUD_SNAPSHOTS_PREFIX)):
                   click.echo("Metadata for volume ID %s is currently unavailable. Skipping import." % volume_id)
                   continue
        click.echo("Scanning metadata for volume ID %s" % volume_id)
        # get latest volume data
        volume_data = cloudutil.get_metadata_object(client, volume_id+'/'+const.CLOUD_VOLUMES_PREFIX)
        
        # import volume metadata
        conn = sqlite_connect(typecfg)
        if volume_data:
            # only import if owner system id matches 
            if not is_global and volume_data['entries'][0]['owner_system_id'] != local_owner_system_id:
                click.echo("Skipping volume ID %s because it is owned by another vSnap" % volume_id)
                continue
            volumes_updated = cloudutil.Importer(**get_volume_import_args(conn, volume_model, typecfg.get_tags_by_vol(volume_id, local_owner_system_id))).restore(volume_data)['updated']
            if volumes_updated > 0:
                click.echo('Imported metadata for volume ID %s' % volume_id)
            volumes_restored += volumes_updated
        # get latest options and snaphsot data
        options_data = cloudutil.get_metadata_object(client, volume_id+'/'+const.CLOUD_VOLUME_OPTIONS_PREFIX)
        snapshot_data = cloudutil.get_metadata_object(client, volume_id+'/'+const.CLOUD_SNAPSHOTS_PREFIX)
        # import options metadata
        if options_data:
            options_updated = cloudutil.Importer(**get_volume_options_import_args(conn, volume_model, volume_options_model)).restore(options_data)['updated']
            click.echo('Imported metadata of %s options for volume ID %s' % (options_updated, volume_id))
        # import snapshot metadata
        if snapshot_data:
            snapshots_updated = cloudutil.Importer(**get_snapshot_import_args(conn, snapshot_model)).restore(snapshot_data)['updated']
            click.echo('Imported metadata of %s snapshots for volume ID %s' % (snapshots_updated, volume_id))
            snapshots_restored += snapshots_updated
        sqlite_disconnect(conn)
        volume_ids.append(volume_id)
    click.echo('Total imported volume metadata objects: %s' % volumes_restored)
    click.echo('Total imported snapshot metadata objects: %s' % snapshots_restored)


def get_volume_options_model(archive=False):
    if VERSION_1015 in CURRENT_VERISON:
        if archive:
            return archcfg.ArchiveVolumeOption
        else:
            return cloudcfg.CloudVolumeOption
    return None


def sqlite_connect(typecfg):
    if VERSION_1016 in CURRENT_VERISON:
        return None
    elif VERSION_1015 in CURRENT_VERISON:
        return typecfg.DBSession()


def sqlite_disconnect(conn):
    if conn != None:
        conn.close()


def get_tags_import_args(conn, volume_model, tag_model):
    if VERSION_1016 in CURRENT_VERISON:
        return {
            'mongo_class': volume_model,
            'sub_mongo_class': tag_model,
            'sub_mongo_class_name': 'tags',
            'sub_mongo_field': 'vol_id'
        }
    elif VERSION_1015 in CURRENT_VERISON:
        return {
            'conn': conn,
            'sqlalchemy_class': tag_model
        }

def get_volume_import_args(conn, volume_model, tags_data):
    if VERSION_1016 in CURRENT_VERISON:
        return {
            'mongo_class': volume_model,
            'include_mongo_class_name': 'tags',
            'include_data': tags_data
        }
    elif VERSION_1015 in CURRENT_VERISON:
        return {
            'conn': conn,
            'sqlalchemy_class': volume_model
        }

def get_volume_options_import_args(conn, volume_model, volume_options_model):
    if VERSION_1016 in CURRENT_VERISON:
        return {
            'mongo_class': volume_model,
            'name_value': True
        }
    elif VERSION_1015 in CURRENT_VERISON:
        return {
            'conn': conn,
            'sqlalchemy_class': volume_options_model
        }

def get_snapshot_import_args(conn, snapshot_model):
    if VERSION_1016 in CURRENT_VERISON:
        return {
            'mongo_class': snapshot_model,
            'sub_mongo_field': 'version'
        }
    elif VERSION_1015 in CURRENT_VERISON:
        return {
            'conn': conn,
            'sqlalchemy_class': snapshot_model
        }


def setup_logging():
    logging.captureWarnings(True)
    logger = logging.getLogger("vsnap")
    logger.setLevel(const.LOG_LEVEL)
    handler = logging.handlers.WatchedFileHandler(const.LOG_FILE)
    handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s pid-%(process)d %(name)s    %(message)s'))
    logger.addHandler(handler)
    logger = logging.getLogger("vsnap.cli")


if __name__ == '__main__':
    setup_logging()
    catalog()
