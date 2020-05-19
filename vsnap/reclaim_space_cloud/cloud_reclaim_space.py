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


@click.command(name="reclaim", help="Reclaim space from the cloud partner.")
@click.option("--partner_id", required=True, help="Partner ID")
@click.option("--days", "num_days", required=True, help="Retention period in days", type=int)
@click.option("--archive", required=False, is_flag=True, help="Cloud bucket is used for archive")
@click.option("--noprompt", is_flag=True, help="Don't prompt for confirmation.")
def reclaim_space(partner_id, num_days, archive, noprompt):
    empty_volumes = 0
    expired_volumes = 0
    reclaimed_snapshots = 0
    volumes_to_delete = []
    volumes_with_reclaimed_snapshots = 0

    cutoff_date = datetime.utcnow() - timedelta(days=num_days)
    if not noprompt:
        click.confirm("Deleting snapshots older than %s. Do you want to continue?" % cutoff_date, abort=True)

    volumes = cloudcore.get_volumes(partner_id, archive=archive)
    total = len(volumes)
    current = 1
    for volume in volumes:
        click.echo("Scanning volume %s of %s: ID %s" % (current, total, volume.id))
        current += 1
        snapshots = cloudcore.get_snapshots(partner_id, volume.id, archive=archive)
        if len(snapshots) == 0:
            empty_volumes += 1
            volumes_to_delete.append(volume)
            logger.info("Volume %s is empty" % volume.id)
            click.echo("Volume ID %s will be deleted because it has no snapshots" % volume.id)
        elif datetime.utcfromtimestamp(int(snapshots[-1].time_created)) < cutoff_date:
            logger.info("Volume %s has no current snapshots and %s expired snapshots" % (volume.id, len(snapshots)))
            click.echo("Volume ID %s will be deleted because it has %s snapshots all of which are expired" % (volume.id, len(snapshots)))
            for snapshot in snapshots:
                click.echo("Cleaning up metadata for snapshot ID %s of volume ID %s" % (snapshot.snap_version, volume.id))
                if archive:
                    archcfg.delete_snapshot(partner_id, snapshot.snap_version, volume.id)
                else:
                    cloudcfg.delete_snapshot(partner_id, snapshot.snap_version, volume.id)
                expired_volumes += 1
            volumes_to_delete.append(volume)
        else:
            reclaimed_snapshots_for_volume = 0
            for snapshot in snapshots:
                created_date = datetime.utcfromtimestamp(int(snapshot.time_created))
                if created_date < cutoff_date:
                    logger.info("Deleting snapshot %s" % snapshot.snap_version)
                    click.echo("Deleting expired snapshot ID %s of volume ID %s" % (snapshot.snap_version, volume.id))
                    cloudcore.delete_snapshot(partner_id, snapshot.snap_version, volume.id, priority=None, archive=archive)
                    reclaimed_snapshots += 1
                    reclaimed_snapshots_for_volume += 1

            if reclaimed_snapshots_for_volume > 0:
                volumes_with_reclaimed_snapshots += 1
                logger.info("Deleted %s snapshots for volume %s" % (reclaimed_snapshots_for_volume, volume.id))
                click.echo("Deleted %s snapshots for volume ID %s" % (reclaimed_snapshots_for_volume, volume.id))

    total = len(volumes)
    current = 1
    volume_delete_errors = 0
    for volume in volumes_to_delete:
        logger.info("Deleting volume %s" % volume.id)
        click.echo("Deleting volume %s of %s: ID %s" % (current, total, volume.id))
        current += 1
        try:
            cloudcore.delete_volume(partner_id, volume.id, archive=archive)
        except Exception as e:
            logger.warning(e)
            logger.warning("Failed to delete volume %s" % volume.id)
            click.echo("Failed to delete volume %s. Continuing." % volume.id)
            volume_delete_errors += 1

    remaining_volumes = len(volumes)-len(volumes_to_delete)+volume_delete_errors
    msg = "Cleanup complete for partner ID %s. Found %s empty volumes. Found %s volumes with expired snapshots. " \
          "Failed to delete %s volumes. There are %s remaining volumes. " \
          "Created sessions to delete %s snapshots from %s volumes. " \
          "Please monitor cloud sessions created to ensure that they complete successfully." % \
          (partner_id, empty_volumes, expired_volumes, volume_delete_errors, remaining_volumes,
           reclaimed_snapshots, volumes_with_reclaimed_snapshots)
    logger.info(msg)
    click.echo(msg)


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
    reclaim_space()
