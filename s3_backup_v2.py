#!/usr/bin/env python3

import argparse
import logging
import multiprocessing as mp
import os
import sys
import time

import cmd_args
import s3backuprestore as s3br

cmp_q = mp.JoinableQueue()
cp_q = mp.JoinableQueue()
tag_q = mp.JoinableQueue()

try:
    cw_dimension_name = os.environ['AWSBatchComputeEnvName']
except KeyError:
    cw_dimension_name = None

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(module)s %(funcName)s" +
           " line %(lineno)d: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
s3br_logger = logging.getLogger('s3backuprestore')
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(parents=[cmd_args.parser])
parser.add_argument(
    '--last-modified-since',
    default=48,
    type=int,
    help='Compare bucket objects if they ' +
         'where modified since the last given hours.',
    required=False,
    metavar='N'
)
parser.add_argument(
    '--tag-deleted',
    action='store_true',
    help='Tag all objects that are deleted in source bucket' +
         'but still present in backup bucket.'
)
cmd_args = parser.parse_args()

ALL = cmd_args.all
CPU_COUNT = mp.cpu_count()
DST_BUCKET = cmd_args.destination_bucket
LAST_MODIFIED_SINCE = cmd_args.last_modified_since
OBJECTS_COUNT = cmd_args.objects_count
PROFILE = cmd_args.profile
REGION = cmd_args.region
SRC_BUCKET = cmd_args.source_bucket
TAG_DELETED = cmd_args.tag_deleted
THREAD_COUNT = cmd_args.thread_count
TIMEOUT = cmd_args.timeout
VERBOSE = cmd_args.verbose

if VERBOSE and VERBOSE == 1:
    logger.setLevel(logging.WARNING)
    s3br_logger.setLevel(logging.WARNING)
elif VERBOSE and VERBOSE == 2:
    logger.setLevel(logging.INFO)
    s3br_logger.setLevel(logging.INFO)
elif VERBOSE and VERBOSE >= 3:
    logger.setLevel(logging.DEBUG)
    s3br_logger.setLevel(logging.DEBUG)

if __name__ == '__main__':
    # Try to set start method of mp
    # environment to spawn. Spawn context is threadsafe
    # and copies only mandatory data to each process.
    try:
        mp.set_start_method('spawn')
        logger.info("Context was set to 'spawn'.")
    except RuntimeError:
        logger.warning("Context already set to 'spawn'.")

    trans_conf = {
        'multipart_threshold': 52428800,
        'multipart_chunksize': 26214400,
        'num_download_attempts': 10,
    }

    # Getting configuration object for backup processes.
    backup_config = s3br.config.Config(
        SRC_BUCKET,
        DST_BUCKET,
        timeout=TIMEOUT,
        last_modified=LAST_MODIFIED_SINCE,
        cw_dimension_name=cw_dimension_name,
        profile_name=PROFILE,
        region=REGION,
        s3_transfer_manager_conf=trans_conf)

    # Getting S3 objects from source bucket
    logger.info("List S3 Keys from {}".format(SRC_BUCKET))
    src_obj = s3br.get_objects(
        SRC_BUCKET,
        config=backup_config,
        objects_count=OBJECTS_COUNT)
    logger.info("{} objects in {}.".format(len(src_obj), SRC_BUCKET))
    logger.debug("Objects: {}".format(src_obj))

    if ALL:
        # Getting objects not in destination bucket
        [cp_q.put(o) for o in src_obj]
        logger.info("{} objects to copy bucket.".format(cp_q.qsize()))
        logger.debug("Objects: {}".format(src_obj))
    else:
        # Getting S3 keys from destiantion bucket
        logger.debug("List S3 Keys from {}".format(DST_BUCKET))
        dst_obj = s3br.get_objects(
            DST_BUCKET,
            config=backup_config)
        logger.info("{} in {}.".format(len(dst_obj), DST_BUCKET))
        logger.debug("Objects: {}".format(dst_obj))

        # Getting objects not in destination bucket
        cp_obj = set(src_obj) - set(dst_obj)
        [cp_q.put(o) for o in cp_obj]
        logger.info("{} objects not in destination bucket."
                    .format(cp_q.qsize()))
        logger.debug("Objects: {}".format(cp_obj))

        # Getting objects to compare between source and destination
        cmp_obj = set(src_obj) & set(dst_obj)
        [cmp_q.put(o) for o in cmp_obj]
        cmp_q_size = cmp_q.qsize()
        logger.info("{} objects to compare.".format(cmp_q_size))
        logger.debug("Objects: {}".format(cmp_obj))

        # Puting metric how many objects to compare
        s3br.put_metric(
            'ObjectsToCompare',
            cmp_q_size,
            config=backup_config)

        # Check if there are any objects to compare.
        if cmp_q_size:
            start = time.time()
            processes = min(cmp_q_size, CPU_COUNT)
            # Starting compare processes
            proc_lst = list()
            logger.info("Starting {} compare processes.".format(processes))
            for p in range(processes):
                proc_lst.append(s3br.MpCompare(
                    config=backup_config,
                    compare_queue=cmp_q,
                    copy_queue=cp_q,
                ))
                proc_lst[p].start()
            logger.info("{} compare processes are started.".format(processes))

            logger.info("Waiting for compare proccesses to be finished.")
            for p in range(processes):
                try:
                    while proc_lst[p].is_alive():
                        logger.debug("{} still alive waiting 1s."
                                     .format(proc_lst[p].name))
                        time.sleep(1)
                        qs = cmp_q.qsize()
                        s3br.put_metric(
                            'ObjectsToCompare', qs, config=backup_config)
                except KeyboardInterrupt:
                    logger.warning("Exiting...")
                    sys.exit(127)
                else:
                    proc_lst[p].join(backup_config.timeout)
                    logger.debug("{} finished.".format(proc_lst[p].name))
            logger.info("All compare processes are finished.")
            logger.info("Comparing objects took {} seconds."
                        .format(time.time() - start))
            s3br.put_metric('ObjectsToCompare', 0, config=backup_config)
        else:
            logger.info("No objects to compare.")

    # Get total number of objects to backup to destination bucket.
    cp_q_size = cp_q.qsize()
    # Puting metric how many objects to backup
    s3br.put_metric(
        'ObjectsToCopy',
        cp_q_size,
        config=backup_config)
    logger.info("{} objects to backup to destination bucket."
                .format(cp_q_size))
    if cp_q_size:
        start = time.time()
        # Starting compare process
        processes = min(cp_q_size, CPU_COUNT)
        proc_lst = list()
        logger.info("Starting {} backup processes.".format(processes))
        for p in range(processes):
            proc_lst.append(s3br.MpBackup(
                config=backup_config,
                copy_queue=cp_q,
                thread_count=25
            ))
            proc_lst[p].start()
        logger.info("{} backup processes are started.".format(processes))

        logger.info("Waiting for backup proccesses to be finished.")
        for p in range(processes):
            try:
                while proc_lst[p].is_alive():
                    logger.debug("{} still alive waiting 1s."
                                 .format(proc_lst[p].name))
                    time.sleep(1)
                    qs = cp_q.qsize()
                    s3br.put_metric(
                        'ObjectsToCopy', qs, config=backup_config)
            except KeyboardInterrupt:
                logger.warning("Exiting...")
                sys.exit(127)
            else:
                proc_lst[p].join(backup_config.timeout)
                logger.debug("{} finished.".format(proc_lst[p].name))
        logger.info("All backup processes are finished.")
        s3br.put_metric('ObjectsToCopy', 0, config=backup_config)
        logger.info("Backup objects took {} seconds."
                    .format(time.time() - start))
    else:
        logger.info("No objects to backup.")

    if TAG_DELETED and not ALL:
        # Getting objects to compare between source and destination
        tag_obj = set(dst_obj) - set(src_obj)
        [tag_q.put(o) for o in tag_obj]
        tag_q_size = tag_q.qsize()
        logger.info("{} objects to tag as deleted".format(tag_q_size))
        logger.debug("Objects: {}".format(tag_obj))

        # Puting metric how many objects to backup
        s3br.put_metric(
            'ObjectsToTagAsDeleted',
            tag_q_size,
            config=backup_config)
        logger.info("{} objects to tag as deleted in destination bucket."
                    .format(tag_q_size))
        if tag_q_size:
            # Starting compare process
            processes = min(tag_q_size, CPU_COUNT)
            proc_lst = list()
            logger.info("Starting {} tag as deleted processes."
                        .format(processes))
            for p in range(processes):
                proc_lst.append(s3br.MpTagDeletedObjects(
                    config=backup_config,
                    tag_queue=tag_q
                ))
                proc_lst[p].start()
            logger.info("{} tagging processes are started.".format(processes))

            logger.info("Waiting for tagging proccesses to be finished.")
            for p in range(processes):
                try:
                    while proc_lst[p].is_alive():
                        logger.debug("{} still alive waiting 1s."
                                     .format(proc_lst[p].name))
                        time.sleep(1)
                        qs = tag_q.qsize()
                        s3br.put_metric(
                            'ObjectsToTagAsDeleted', qs, config=backup_config)
                except KeyboardInterrupt:
                    logger.warning("Exiting...")
                    sys.exit(127)
                else:
                    proc_lst[p].join(backup_config.timeout)
                    logger.debug("{} finished.".format(proc_lst[p].name))
                logger.info("All tagging processes are finished.")
            s3br.put_metric('ObjectsToTagAsDeleted', 0, config=backup_config)
        else:
            logger.info("No objects to to tag.")
