This script works on versions: 10.1.3, 10.1.4, 10.1.5.

Use this script to reclaim space in a vSnap storage pool by deleting older
snapshots that may have been left behind due to problems with the SPP
Maintenance job.

The script queues snapshots/volumes for background deletion which is then
handled by the vsnap-maint service. Depending on the number of objects
queued for deletion, the service may take several hours or even days to
work through the queue. Space reclamation occurs gradually as the objects
are deleted.

When replication is in use, run the script on the primary server first, then
on the replica server. If replication is configured such that a single
vSnap can act as both primary and replica, then perform two runs of the
script. For example, run the script once on vSnap servers V1, V2, and V3,
and then perform a second run of the script on the same servers in the same
sequence V1, V2, and V3. During the first run, there may be errors about
locked snapshots which is expected. There should be much fewer errors
during the second run.

To run the script, upload it to each vSnap server and perform the following:

# Temporarily stop the vSnap Maintenance service
sudo systemctl stop vsnap-maint

# Make the script executable
chmod +x reclaim_space_local.sh

# Run the script and collect the output in a log file
# Replace <numDays> with the retention period in days
# The script will clean up snapshots older than <numDays>

sudo ./reclaim_space_local.sh <numDays> | tee -a reclaim_local_output.log

# Restart the vSnap Maintenance service
sudo systemctl start vsnap-maint
