#!/bin/bash

# This script frees up space in the vSnap storage pool by finding and deleting
# snapshots older than the specified number of days.

if [[ -z $1 ]]; then
	echo "Usage: reclaim_space_local.sh <numDays>"
	exit 1
fi

# If not running on 10.1.3, 10.1.4, or 10.1.5, throw an error.
vsnapVers=$(rpm -q --qf "%{VERSION}" vsnap 2>/dev/null)
if [[ $vsnapVers != "10.1.3" ]] && [[ $vsnapVers != "10.1.4" ]] && [[ $vsnapVers != "10.1.5" ]]; then
	echo "Error: Detected unsupported vSnap version: $vsnapVers"
	echo "       Supported version: 10.1.5"
	exit 1
fi

# Get the cutoff date by finding the current epoch time and subtracting numDays

numDays=$1
numSeconds=$(expr 86400 '*' $numDays)
currDate=$(date +%s)
cutoffDate=$(expr $currDate - $numSeconds)

echo "Hostname: $(hostname)"
echo "Deleting snapshots older than $cutoffDate ($(date --date='@'$cutoffDate))"
read -r -p "Do you want to continue ? [y/N] " response
if [[ ! $response =~ ^([yY][eE][sS]|[yY])$ ]] ; then
	exit 1
fi

# Loop over all snapshots and delete anything that has a creation date older than
# the cutoff

CONFIGDB=/etc/vsnap/config.db

sqlite3 $CONFIGDB "select id from snapshots where created < '$cutoffDate';" | while read id; do
	echo "Deleting snapshot ID $id"
	vsnap snapshot delete --id $id --noprompt
done

# Now that we've deleted some number of snapshots, look for volumes that have zero snapshots
# We can delete the entire volume if it has no snapshots. If the volume has a replication
# relationship, we can delete the relationship. This won't impact data on the replica server.

REPLICADB=/etc/vsnap/replica.db
CLOUDDB=/etc/vsnap/cloud_v2.db
ARCHIVEDB=/etc/vsnap/archive_v2.db

sqlite3 $CONFIGDB "select id from volumes;" | while read id; do
	numSnaps=$(sqlite3 $CONFIGDB "select count(id) from snapshots where parent_vol=$id;")
	if [[ $numSnaps == 0 ]]; then
		echo "Deleting volume ID $id because it has no snapshots"
		# If volume is vsnap_metadata_cloud, skip it
		volName=$(sqlite3 $CONFIGDB "select name from volumes where id=$id;")
		if [[ $volName == "vsnap_metadata_cloud" ]]; then
			echo "Skipped deleting volume ID $id because it is reserved for internal metadata storage"
			continue
		fi
		# If volume is a restore clone, skip it
		isClone=$(sqlite3 $CONFIGDB "select value from volume_options where id=$id and name='snapshot_id';" | wc -l)
		if [[ $isClone != 0 ]]; then
			echo "Skipped deleting volume ID $id because it is a clone and may be in use by active restore jobs"
			continue
		fi
		sqlite3 $REPLICADB "select id from relationships where local_vol_id=$id;" | while read rel; do
			echo "Deleting replication relationship on volume ID $id"
			vsnap relationship delete --id $rel --preserve_replica --force --noprompt
		done
		sqlite3 $CLOUDDB "select id from relationships_cloud where local_vol_id=$id;" | while read rel; do
			echo "Deleting cloud relationship on volume ID $id"
			vsnap cloud relationship delete --id $rel --noprompt
		done
		sqlite3 $ARCHIVEDB "select id from relationships_archive where local_vol_id=$id;" | while read rel; do
			echo "Deleting archive relationship on volume ID $id"
			vsnap archive relationship delete --id $rel --noprompt
		done
		vsnap volume delete --id $id --force --noprompt
	fi
done
