#!/usr/bin/env python3

import argparse
import logging
import multiprocessing as mp
import sys
import time

import cmd_args
import s3backuprestore as s3br

manager = mp.Manager()
rst_q = manager.Queue()
check_deleted_q = manager.Queue()

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
    '--check-deleted-tag',
    action='store_true',
    help='Restore objects which are not tagged as deleted.'
)
cmd_args = parser.parse_args()

ALL = cmd_args.all
CHECK_DELETED_TAG = cmd_args.check_deleted_tag
CPU_COUNT = mp.cpu_count()
CW_DIMENSION_NAME = cmd_args.cloudwatch_dimension_name
DST_BUCKET = cmd_args.destination_bucket
OBJECTS_COUNT = cmd_args.objects_count
PROFILE = cmd_args.profile
REGION = cmd_args.region
SRC_BUCKET = cmd_args.source_bucket
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
    # Try to set start method of multiprocessing
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

    # Getting configuration object for restore processes.
    restore_config = s3br.config.Config(
        SRC_BUCKET,
        DST_BUCKET,
        timeout=TIMEOUT,
        cw_dimension_name=CW_DIMENSION_NAME,
        profile_name=PROFILE,
        region=REGION,
        s3_transfer_manager_conf=trans_conf)

    # Getting S3 objects from source bucket
    logger.info("List S3 Keys from {}".format(SRC_BUCKET))
    src_obj = s3br.get_objects(
        SRC_BUCKET,
        config=restore_config,
        objects_count=OBJECTS_COUNT)
    logger.info("{} objects in {}.".format(len(src_obj), SRC_BUCKET))

    if ALL:
        [rst_q.put(o) for o in src_obj]
        logger.info("{} objects to restore".format(rst_q.qsize()))
    elif CHECK_DELETED_TAG:
        [check_deleted_q.put(o) for o in src_obj]
        check_deleted_q_size = check_deleted_q.qsize()
        logger.info("{} objects to check for 'Deleted' tag."
                    .format(check_deleted_q_size))

        # Puting metric how many objects to compare
        s3br.put_metric(
            'ObjectsToCheckForDeletedTag',
            check_deleted_q_size,
            config=restore_config)

        # Check if there are any objects to check for deleted tag.
        if check_deleted_q_size:
            start = time.time()
            # Starting compare processes
            processes = min(check_deleted_q_size, CPU_COUNT)
            proc_lst = list()
            logger.info("Starting {} check for deleted tag processes."
                        .format(processes))
            for p in range(processes):
                proc_lst.append(s3br.MpCheckDeletedTag(
                    config=restore_config,
                    check_queue=check_deleted_q,
                    checked_queue=rst_q,
                    thread_count=25
                ))
                proc_lst[p].start()
            logger.info("{} check for deleted tag processes are started."
                        .format(processes))

            logger.info("Waiting for deleted tag proccesses to be finished.")
            for p in range(processes):
                try:
                    while proc_lst[p].is_alive():
                        logger.debug("{} still alive waiting 60s."
                                     .format(proc_lst[p].name))
                        time.sleep(60)
                        qs = check_deleted_q.qsize()
                        s3br.put_metric(
                            'ObjectsToCheckForDeletedTag',
                            qs,
                            config=restore_config)
                except KeyboardInterrupt:
                    logger.warning("Exiting...")
                    sys.exit(127)
                else:
                    proc_lst[p].join(restore_config.timeout)
                    logger.debug("{} finished.".format(proc_lst[p].name))
            logger.info("All check deleted tag processes are finished.")
            logger.info("Check for deleted tag took {} seconds."
                        .format(time.time() - start))
            s3br.put_metric(
                'ObjectsToCheckForDeletedTag',
                0,
                config=restore_config)
        else:
            logger.info("No objects to check for deleted tag.")

    # Get total number of objects to restore to destination bucket.
    rst_q_size = rst_q.qsize()
    # Puting metric how many objects to backup
    s3br.put_metric(
        'ObjectsToRestore',
        rst_q_size,
        config=restore_config)
    logger.info("{} objects to restore to destination bucket."
                .format(rst_q_size))
    if rst_q_size:
        start = time.time()
        # Starting compare process
        processes = min(rst_q_size, CPU_COUNT)
        proc_lst = list()
        logger.info("Starting {} retore processes.".format(processes))
        for p in range(processes):
            proc_lst.append(s3br.MpRestore(
                config=restore_config,
                copy_queue=rst_q,
                thread_count=25
            ))
            proc_lst[p].start()
        logger.info("{} restore processes are started.".format(CPU_COUNT))

        logger.info("Waiting for restore proccesses to be finished.")
        for p in range(processes):
            try:
                while proc_lst[p].is_alive():
                    logger.debug("{} still alive waiting 60s."
                                 .format(proc_lst[p].name))
                    time.sleep(60)
                    qs = rst_q.qsize()
                    s3br.put_metric(
                        'ObjectsToRestore', qs, config=restore_config)
            except KeyboardInterrupt:
                logger.warning("Exiting...")
                sys.exit(127)
            else:
                proc_lst[p].join(restore_config.timeout)
                logger.debug("{} finished.".format(proc_lst[p].name))
        logger.info("All restore processes are finished.")
        s3br.put_metric('ObjectsToRestore', 0, config=restore_config)
        logger.info("Restoring objects took {} seconds."
                    .format(time.time() - start))
    else:
        logger.info("No objects to restore.")
