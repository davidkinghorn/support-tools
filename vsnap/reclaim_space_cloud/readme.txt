These scripts works on versions: 10.1.4, 10.1.5.

Use these scripts to reclaim space in a cloud or repository server by deleting
older snapshots that may have been left behind due to problems with the SPP
Maintenance job.

Upload the scripts to each vSnap server. Make them executable:

    chmod +x cloud_metadata_import.py
    chmod +x cloud_reclaim_space.py

Temporarily remove existing metadata associated with all cloud partners:

    vsnap --detail cloud partner show | grep ^ID | awk '{print $2}' | while read partner; do echo "Removing partner $partner"; vsnap cloud partner remove --id $partner --noprompt; done

Or for a specific partner only:

    vsnap cloud partner remove --id <ID>

Run the first script to import all cloud metadata from the bucket:

    sudo ./cloud_metadata_import.py --is_global | tee -a import_output.log

Enter the endpoint details of the cloud partner when prompted. Example:

    Endpoint URL: https://172.20.46.6:9000
    Access Key: ABC
    Secret Key: XYZ
    Bucket: spp
    Provider Type: cos

The script scans the bucket and re-imports metadata for all volumes/snapshots
found in the bucket. This may take several minutes to complete.

Once the first script completes, run the following and make a note of the
partner ID:

    vsnap cloud partner show

Run the next script to clean up all snapshots older than the retention period.
Replace <ID> with the partner ID noted in the previous step.
Replace <numDays> with the retention period in days. The script will clean up
snapshots older than <numDays> and will clean up leftover volumes that have
no remaining snapshots.

    sudo ./cloud_reclaim_space.py --partner_id <ID> --days <numDays> | tee -a reclaim_output.log

The script queues snapshots for background deletion which are then
handled by the vsnap-repl service. Depending on the number of objects
queued for deletion, the service may take several hours or even days to
work through the queue. Space reclamation occurs gradually as the objects
are deleted. The status of the deletion session can be monitored using:

    vsnap cloud session show --action delete

Once all deletion sessions are complete, remove the cloud partner:

    vsnap cloud partner remove --id <ID>

Then add it again, this time without the 'is_global' flag. This ensures
that metadata associated only with the current vSnap is imported.

    sudo ./cloud_metadata_import.py --is_global

Enter the endpoint details of the cloud partner when prompted.
