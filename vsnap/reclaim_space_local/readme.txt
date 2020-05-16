Use this script to reclaim space in a vSnap storage pool by deleting older
snapshots that may have been left behind due to problems with the SPP
Maintenance job.

Upload the script to each vSnap server and perform the following steps.

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

The script queues snapshots/volumes for background deletion which is then
handled by the vsnap-maint service. Depending on the number of objects
queued for deletion, the service may take several hours or even days to
work through the queue. Space reclamation occurs gradually as the objects
are deleted.
