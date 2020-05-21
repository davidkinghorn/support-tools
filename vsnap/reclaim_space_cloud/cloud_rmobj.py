#!/opt/vsnap/venv/bin/python3

import sys
sys.path.append('/opt/vsnap/lib')
import click
import logging
import logging.handlers

from datetime import datetime, timedelta
from vsnap import config, core
from vsnap.common import const
from vsnap.linux import system

from vsnap.cloud import core as cloudcore
from vsnap.cloud import util as cloudutil
from vsnap.cloud import client as cloudclient

CURRENT_VERISON = system.get_package_version("vsnap")
VERSION_1016 = '10.1.6'
VERSION_1015 = '10.1.5'
VERSION_1014 = '10.1.4'

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


@click.command(name="rmobj", help="Reclaim space from the cloud partner.")
@click.option("--partner_id", required=True, help="Partner ID")
@click.option("--prefix", required=False, help="Prefix to delete. If this is omitted, the entire bucket will be emptied.")
@click.option("--archive", is_flag=True, help="Cloud bucket is used for archive")
@click.option("--noprompt", is_flag=True, help="Don't prompt for confirmation.")
def rmobj(partner_id, prefix, archive, noprompt):
    if not noprompt:
        click.confirm("Deleting all objects under %s. Do you want to continue?" % prefix, abort=True)

    cloudcore.cloud_remove_objects(partner_id=partner_id, prefix=prefix, archive=archive)


def setup_logging():
    global logger
    logging.captureWarnings(True)
    logger = logging.getLogger("vsnap")
    logger.setLevel(const.LOG_LEVEL)
    handler = logging.handlers.WatchedFileHandler(const.LOG_FILE)
    handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s pid-%(process)d %(name)s    %(message)s'))
    logger.addHandler(handler)
    logger = logging.getLogger("vsnap.cli")


if __name__ == '__main__':
    setup_logging()
    rmobj()
