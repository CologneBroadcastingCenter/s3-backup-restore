# s3-backup-restore

_s3-backup-restore_ — This repository contains an easy to use module to backup,
restore, compare, and tag S3 object keys. It contains as well two scripts that
will do the actual workload.
All scripts are written in __Python 3.6__ and using __Boto 3__

## Prerequisite / Installation

You need at least __Python 3.6__ and __Boto 3__
If you want to use this module have to install all modules specified in the
__requirements.txt__ file.

`pip install -r requirements.txt`

## s3backuprestore Module

This module contains the actual logic that will allow you to write scripts that
will backup your bucket as fast as possible or Restore objects that will be
transmissioned from Glacier.
There are lot of classes that have the ability to help you comming arround with
performance issues.

### Getting started

Sample usage in your script.

```python
import s3backuprestore as s3br
import multiprocessing as mp


def main():
    manager = mp.Manager()
    backup_queue = manager.Queue()

    # Configure the module
    config = s3br.config.Config(
        src_bucket='backup-source-bucket',
        dst_bucket='backup-destination-bucket',
        last_modified=24)

    obj_to_backup = s3br.get_objects(
        config.src_bucket,
        config=config)

    [backup_queue.put(o) for o in obj_to_backup]

    # Sending queue size of objects to copy to CloudWatch
    s3br.put_metric(
        'ObjectsToBackup',
        backup_queue.qsize(),
        config=config)

    if backup_queue.qsize() > 0:
        # Starting 4 processes
        proc_lst = list()
        for p in range(4):
            proc_lst.append(s3br.MpBackup(
                config=config,
                backup_queue=backup_queue,
                thread_count=10))
            proc_lst[p].start()

        for p in range(4):
                while proc_lst[p].is_alive():
                    qs = backup_queue.qsize()
                    s3br.put_metric(
                        'ObjectsToBackup',
                        qs,
                        config=config)
        else:
            proc_lst[p].join(config.timeout)
        s3br.put_metric('ObjectsToBackup', 0, config=config)


if __name__ == '__main__':
    main()

```

The sample above shows you how to setup a short sample script that will start
4 Python processes and in each those processes there will be 10 threads.

### config

The config Class provides an interface that will give you the possibility to
configure the other objects by creating a config class and passing it to the
other classes like _backup_ or _restore_.

### backup

The MpBackup Class needs a copy_queue and configured config class.
It will the spawn as much threads as specified and will then start copying
all objects from _src\_bucket_ to _dest\_bucket_.
It will send in data in a regular basis how much S3 objects are still to backup.

### compare

The MpCompare Class needs two queues, one input and an output queue and
the configured config class. It will consumes the input queues object keys and compare them by checking their object size and last modified header fields. If those received values are differend between _src\_bucket_ and _dst\_bucket_ the class will add those keys to the output queue.

### tagging

The tagging module provides to Classes one that will Tag objects in backup.
The other checks if "Deleted" Tag exists and its Value is "True.

For the first Class MpTagDeletedObjects it needs a queue with objects to tag
and configured config object.
After objects are provided it will tag all objects with mentioned TagSet.

```json
{
    "Key": "Deleted",
    "Value": "True"
},
{
    "Key": "DeletedAt",
    "Value": "YYYY-MM-DDTHH:MM"
}
```

The second Class MpCheckDeletedTag needs two queues an input and output queue as
well as the confiugrued config object.
The Class will compare all S3 object keys from the input queue and only will
add those S3 objects to the output queue if they __do not__ have the "Deleted"
tag set to "True"

### restore

The MpRestore Class needs a restore_queue and the configured config class.
It restores all objects from _src\_bucket_ to a newly created bucket.
Before the actual copy mechanism is done it checks objects in which S3 storage
class they are. If the are in storage class __GLACIER__ and there is no
ongoing request or the ongoing request is still in process it will put back
this object to the queue and it will try to restore it later on.

### Helper Functions

There are some helper functions like publishing metric to _CloudWatch_ or
listing object from S3 buckets. Those interfaces are described in docstring
format.

## s3_backup.py

This script uses the s3backuprestore module to glue everything together to
become a complete backup script. It lists all S3 object from source and
destiantion bucket.
It checks first if there are any objects that aren't in the backup and added
them to the backup queue.
After that it will compare all objects which are in both buckets, if they
differe in case of size or last modified it will also add those s3 object to
the backup queue.
If both processes are successful the script will copy all objects stored in the
backup queue.

To tag objects that are deleted from the source bucket the script makes a diff
and adds those objects back to a tagging queue and starts the tagging processes.

If everything works and suceeded the backup is finished.

## s3_restore.py

This script uses s3backuprestore as well as `s3_backup.py`.
It lists all objects from backup bucket, checks if those objects are tagged as
"Deleted": "True" if not they will be restored to a __newly__ created bucket.
