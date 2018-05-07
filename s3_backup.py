import boto3
import argparse
import logging
import queue
import sys
import threading
import time
import os
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError, ParamValidationError

copy_queue = queue.Queue()
comparison_queue = queue.Queue()
deleted_keys_queue = queue.Queue()
deletion_count_queue = queue.Queue()

# Cloudwatch Configuration
cw_namespace = 'BackupRecovery'

try:
    cw_dimension_name = os.environ['AWSBatchComputeEnvName']
except KeyError:
    cw_dimension_name = 'Dev'

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(funcName)s line %(lineno)d: %(message)s"
)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
    '--source-bucket',
    '-s',
    help='Source Bucket to sync diff from.',
    required=True)
parser.add_argument(
    '--destination-bucket',
    '-d',
    help='Destination Bucket to sync to.',
    required=True)
parser.add_argument(
    '--prefix',
    default='',
    help='Prefix to list objects from and to sync to.')
parser.add_argument(
    '--last-modified-since',
    default=48,
    type=int,
    help='Compare bucket objects if they where modified since the last given hours. If so copy them to backup.',
    required=False,
    metavar='N')
parser.add_argument(
    '--tag-deleted',
    action='store_true',
    help='Tag all objects that are deleted in source bucket but still present in backup bucket.')
parser.add_argument(
    '--thread-count',
    default=10,
    metavar='N',
    type=int,
    help='Starting count for threads.')
parser.add_argument(
    '--profile',
    '-p',
    help='AWS profile name configure in aws config files.')
parser.add_argument(
    '--region',
    default='eu-central-1',
    help='AWS Region')
parser.add_argument(
    '--verbose', '-v',
    action='count')
parser.add_argument(
    '--copy-num-objects',
    help='[DEBUGGING] Number of objecst to copy.',
    required=False,
    type=int,
    metavar='N')
cmd_args = parser.parse_args()

PROFILE = cmd_args.profile
SOURCE_BUCKET = cmd_args.source_bucket
DESTINATION_BUCKET = cmd_args.destination_bucket
LAST_MODIFIED_SINCE = cmd_args.last_modified_since
TAG_DELETED = cmd_args.tag_deleted
THREAD_COUNT = cmd_args.thread_count
AWS_REGION = cmd_args.region
VERBOSE = cmd_args.verbose
COPY_NUM_OBJECTS = cmd_args.copy_num_objects

if VERBOSE and VERBOSE == 1:
    logger.setLevel(logging.WARNING)
elif VERBOSE and VERBOSE == 2:
    logger.setLevel(logging.INFO)
elif VERBOSE and VERBOSE >= 3:
    logger.setLevel(logging.DEBUG)


class S3Backup(threading.Thread):
    def __init__(self, thread_number, aws_session, source_bucket, destination_bucket, cw_namespace, cw_dimension_name):
        threading.Thread.__init__(self)
        self.thread_number = thread_number
        self.aws_session = aws_session
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.transfere_config = boto3.s3.transfer.TransferConfig(
            use_threads=True,
            max_concurrency=10
        )
        self.cw_namespace = cw_namespace
        self.cw_dimension_name = cw_dimension_name
        self.cw_metric_name = 'BucketObjectsToCopyErrors'

    def run(self):
        global copy_queue
        self.s3 = self.aws_session.resource('s3')

        while not copy_queue.empty():
            self.key = copy_queue.get()

            # Preparing copy task
            self.dest_obj = self.s3.Object(self.destination_bucket, self.key)
            logger.info("Thread number {} copying key: {}".format(self.thread_number, self.key))
            try:
                self.copy_source = {'Bucket': self.source_bucket, 'Key': self.key}
                self.dest_obj.copy(
                    self.copy_source,
                    Config=self.transfere_config
                )
                copy_queue.task_done()
            except ConnectionRefusedError as exc:
                logger.error("To many connections open.\n\
                            Put {} back to queue.".format(self.key))
                copy_queue.put(self.key)
            except:
                logger.exception("Unhandeld exception occured.\n\
                            Put {} back to queue.".format(self.key))
                self.cw_put_error()
                copy_queue.put(self.key)

    def cw_put_error(self):
        self.cw = self.aws_session.resource('cloudwatch')
        self.statistic_value = 1
        try:
            float(self.statistic_value)
        except ValueError:
            logger.error("Statistic Value not convertibal to float")

        try:
            if self.statistic_value != 0:
                cw.Metric(self.cw_namespace, self.cw_metric_name).put_data(
                    MetricData=[
                        {
                            'MetricName': self.cw_metric_name,
                            'Dimensions': [
                                {
                                    'Name': self.cw_dimension_name,
                                    'Value': self.cw_metric_name
                                }
                            ],
                            'StatisticValues': {
                                'SampleCount': self.statistic_value,
                                'Sum': self.statistic_value,
                                'Minimum': self.statistic_value,
                                'Maximum': self.statistic_value
                            },
                            'Unit': 'Count',
                            'StorageResolution': 1
                        }
                    ]
                )
        except InvalidParameterValue:
            logger.error("StatisticValue is not well formed or equal 0")
        except:
            logger.exception("")


class CompareBucketObjects(threading.Thread):
    def __init__(self, thread_number, aws_session, source_bucket, dest_bucket, cw_namespace, cw_dimension_name, last_modified_since=48):
        threading.Thread.__init__(self)
        self.thread_number = thread_number
        self.aws_session = aws_session
        self.source_bucket = source_bucket
        self.dest_bucket = dest_bucket
        self.last_modified_since = last_modified_since
        self.time_obj = datetime.now(timezone.utc)-timedelta(hours=self.last_modified_since)
        self.cw_namespace = cw_namespace
        self.cw_dimension_name = cw_dimension_name
        self.cw_metric_name = 'BucketObjectsToCompareErrors'

    def run(self):
        global copy_queue
        global comparison_queue
        try:
            self.s3 = self.aws_session.resource('s3')
        except:
            logger.exception("")
        while not comparison_queue.empty():
            self.key = comparison_queue.get()

            try:
                self.src_lm = self.s3.Object(self.source_bucket, self.key).last_modified
                logger.debug("Source LastModified: {}".format(self.src_lm))

                self.src_cl = self.s3.Object(self.source_bucket, self.key).content_length
                self.dst_cl = self.s3.Object(self.dest_bucket, self.key).content_length
                logger.debug("Source ContentLength: \t{}\nDestinatin ContentLength: \t{}".format(self.src_cl, self.dst_cl))
            except ConnectionRefusedError as exc:
                logger.error("To many connections open.\n\
                Put {} back to queue.".format(self.key))
                comparison_queue.put(self.key)
            except:
                logger.exception("Unhandeld exception occured.\n\
                Put {} back to queue.".format(self.key,))
                self.cw_put_error()
                comparison_queue.put(self.key)
            else:
                if self.src_cl != self.dst_cl:
                    logger.info("Objects content lengths are unequal.\n\
                    Adding {} to queue.".format(self.key))
                    copy_queue.put(self.key)
                elif self.src_lm > self.time_obj:
                    logger.info("Object modified within last {}hours.\n\
                    Adding {} to queue.".format(self.last_modified_since, self.key))
                    copy_queue.put(self.key)
                comparison_queue.task_done()

    def cw_put_error(self):
        self.cw = self.aws_session.resource('cloudwatch')
        self.statistic_value = 1
        try:
            float(self.statistic_value)
        except ValueError:
            logger.error("Statistic Value not convertibal to float")

        try:
            if self.statistic_value != 0:
                cw.Metric(self.cw_namespace, self.cw_metric_name).put_data(
                    MetricData=[
                        {
                            'MetricName': self.cw_metric_name,
                            'Dimensions': [
                                {
                                    'Name': self.cw_dimension_name,
                                    'Value': self.cw_metric_name
                                }
                            ],
                            'StatisticValues': {
                                'SampleCount': self.statistic_value,
                                'Sum': self.statistic_value,
                                'Minimum': self.statistic_value,
                                'Maximum': self.statistic_value
                            },
                            'Unit': 'Count',
                            'StorageResolution': 1
                        }
                    ]
                )
        except InvalidParameterValue:
            logger.error("StatisticValue is not well formed or equal 0")
        except:
            logger.exception("")


class TagDeletedKeys(threading.Thread):
    def __init__(self, thread_number, aws_session, destination_bucket, cw_namespace, cw_dimension_name):
        threading.Thread.__init__(self)
        self.thread_number = thread_number
        self.aws_session = aws_session
        self.destination_bucket = destination_bucket
        self.cw_namespace = cw_namespace
        self.cw_dimension_name = cw_dimension_name
        self.cw_metric_name = 'BucketObjectsToTagAsDeletedErrors'

    def run(self):
        global deleted_keys_queue
        global deletion_count_queue
        self.s3 = self.aws_session.resource('s3')
        self.deleted_count = 0

        while not deleted_keys_queue.empty():
            self.key = deleted_keys_queue.get()
            self.deleted_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            self.deleted = True
            self.deleted_at = True

            try:
                logger.debug("Getting tagging information from {}".format(self.key))
                self.resp = self.s3.meta.client.get_object_tagging(
                    Bucket=self.destination_bucket,
                    Key=self.key
                )
            except ConnectionRefusedError:
                logger.error("To many connections open.\n\
                Put {} back to queue.".format(self.key))
                deleted_keys_queue.put(self.key)
                continue
            except:
                logger.exception("Unhandeld exception occured.\n\
                Put {} back to queue.".format(self.key))
                self.cw_put_error()
                deleted_keys_queue.put(self.key)

            for self.tag_key in self.resp['TagSet']:
                logger.debug("TagSet for key {}: {}".format(self.key, self.tag_key))
                try:
                    if self.tag_key['Key'] == 'Deleted':
                        self.deleted = False
                        logger.debug("Tag deleted exists. Set to False")
                    if self.tag_key['Key'] == 'DeletedAt':
                        self.deleted_at = False
                        logger.debug("Tag DeletedAt exists. Set to False")
                except KeyError:
                    logger.debug("Object {} has no tags.".format(self.key))

            if self.deleted or self.deleted_at:
                logger.debug("Thread number {} tagging key: {}".format(self.thread_number, self.key))
                self.kwargs = {
                    'Bucket': self.destination_bucket,
                    'Key': self.key,
                    'Tagging': {
                        'TagSet': [
                            {
                                'Key': 'Deleted',
                                'Value': 'True'
                            },
                            {
                                'Key': 'DeletedAt',
                                'Value': self.deleted_time
                            }
                        ]
                    }
                }
                logger.info("{} will be tagged as deleted.".format(self.key))
                try:
                    self.resp = self.s3.meta.client.put_object_tagging(**self.kwargs)
                    deleted_keys_queue.task_done()
                    self.deleted_count += 1
                except:
                    deleted_keys_queue.put(self.key)
                    logger.exception("")
            else:
                deleted_keys_queue.task_done()
        deletion_count_queue.put(self.deleted_count)
        logger.debug("Thread {} as marked {} keys as deleted".format(thread_num, self.deleted_count))

    def cw_put_error(self):
        self.cw = self.aws_session.resource('cloudwatch')
        self.statistic_value = 1
        try:
            float(self.statistic_value)
        except ValueError:
            logger.error("Statistic Value not convertibal to float")

        try:
            if self.statistic_value != 0:
                cw.Metric(self.cw_namespace, self.cw_metric_name).put_data(
                    MetricData=[
                        {
                            'MetricName': self.cw_metric_name,
                            'Dimensions': [
                                {
                                    'Name': self.cw_dimension_name,
                                    'Value': self.cw_metric_name
                                }
                            ],
                            'StatisticValues': {
                                'SampleCount': self.statistic_value,
                                'Sum': self.statistic_value,
                                'Minimum': self.statistic_value,
                                'Maximum': self.statistic_value
                            },
                            'Unit': 'Count',
                            'StorageResolution': 1
                        }
                    ]
                )
        except InvalidParameterValue:
            logger.error("StatisticValue is not well formed or equal 0")
        except:
            logger.exception("")


def get_s3_keys(aws_session, bucket, copy_num_objects=None):
    logger.warning("Listing {} for objects.".format(bucket))
    keys = list()
    start = time.time()
    try:
        for key in aws_session.resource('s3').Bucket(bucket).objects.all():
            keys.append(key.key)

            if time.time()-1 > start:
                logger.warning("Collected keys: {}".format(len(keys)))
                start = time.time()

            # Break condition to escape earlier thant complete bucket listing
            if copy_num_objects and len(keys) >= copy_num_objects:
                break
        logger.warning("Summary of collected keys {}".format(len(keys)))
    except:
        logger.exception("")
        sys.exit(127)
    else:
        return keys


def cw_put_queue_count(aws_session, cw_namespace, cw_metric_name, cw_dimension_name, statistic_value):
    cw = aws_session.resource('cloudwatch')
    try:
        float(statistic_value)
    except ValueError:
        logger.error("Statistic Value not convertibal to float")

    try:
        if statistic_value != 0:
            cw.Metric(cw_namespace, cw_metric_name).put_data(
                MetricData=[
                    {
                        'MetricName': cw_metric_name,
                        'Dimensions': [
                            {
                                'Name': cw_dimension_name,
                                'Value': cw_metric_name
                            }
                        ],
                        'StatisticValues': {
                            'SampleCount': statistic_value,
                            'Sum': statistic_value,
                            'Minimum': statistic_value,
                            'Maximum': statistic_value
                        },
                        'Unit': 'Count',
                        'StorageResolution': 1
                    }
                ]
            )
    except InvalidParameterValue:
        logger.error("StatisticValue is not well formed or equal 0")
    except:
        logger.exception("")


if __name__ == '__main__':
    try:
        if PROFILE:
            aws_session = boto3.session.Session(profile_name=PROFILE, region_name=AWS_REGION)
            cred = aws_session.get_credentials().get_frozen_credentials()
        else:
            aws_session = boto3.session.Session(region_name=AWS_REGION)
        try:
            cw_res = aws_session.resource('cloudwatch')
        except:
            logger.exception("")
            sys.exit(127)
    except KeyboardInterrupt:
        logger.warning("Exiting...")
        sys.exit(127)
    except ClientError:
        logger.warning("Exiting program. Wrong MFA token!")
        sys.exit(127)
    except ParamValidationError:
        logger.warning("Exiting program. Empty MFA token!")
        sys.exit(127)
    except:
        logger.exception("")
        sys.exit(127)
    else:
        # Getting S3 keys from source bucket
        logger.debug("List S3 Keys from {}".format(SOURCE_BUCKET))
        source_bucket_keys = get_s3_keys(aws_session, SOURCE_BUCKET, copy_num_objects=COPY_NUM_OBJECTS)

        # Getting S3 keys from source bucket
        logger.debug("List S3 Keys from {}".format(DESTINATION_BUCKET))
        dest_bucket_keys = get_s3_keys(aws_session, DESTINATION_BUCKET)

        # Sending keys to queue if they are not in Destination Bucket
        keys_not_in_dest_bucket = set(source_bucket_keys) - set(dest_bucket_keys)
        [copy_queue.put(key) for key in keys_not_in_dest_bucket]
        logger.warning("{} key(s) not in destination Bucket.".format(len(keys_not_in_dest_bucket)))
        logger.debug("Keys not in destination Bucket: {}".format(keys_not_in_dest_bucket))

        # Check all keys existing in destination bucket and source bucket
        keys_to_check = set(source_bucket_keys) & set(dest_bucket_keys)
        [comparison_queue.put(key) for key in keys_to_check]
        logger.warning("{} keys to check for equality.".format(len(keys_to_check)))
        logger.debug("Keys not in destination Bucket: {}".format(keys_to_check))

        # Check if there are any objects to compare.
        comparison_queue_size = comparison_queue.qsize()
        logger.info("Comparison queue size: {}".format(comparison_queue_size))
        # Put comparison_queue_size to Cloudwatch
        cw_put_queue_count(aws_session, cw_namespace, 'BucketObjectsToCompare', cw_dimension_name, comparison_queue_size)
        if comparison_queue_size > 0:
            # Check if thread_count_size is less than THREAD_COUNT
            # If so set THREAD_COUNT to thread_count_size
            if comparison_queue_size < THREAD_COUNT:
                thread_count = comparison_queue_size
            else:
                thread_count = THREAD_COUNT

            # Start equality check of objects in S3
            # Puting objects not equal into copy_queue
            th = list()
            logger.warning("Starting comparison.")
            logger.info("Generating {} comparison threads.".format(thread_count))
            start = time.time()
            for thread_num in range(0, thread_count):
                th.append(CompareBucketObjects(
                    thread_num,
                    aws_session,
                    SOURCE_BUCKET,
                    DESTINATION_BUCKET,
                    cw_namespace,
                    cw_dimension_name,
                    LAST_MODIFIED_SINCE))
                th[thread_num].daemon = True
                th[thread_num].start()

            logger.warning("Waiting for comparison queue to be finished.")
            try:
                while True:
                    if not comparison_queue.empty():
                        time.sleep(15)
                        qs = comparison_queue.qsize()
                        cw_put_queue_count(aws_session, cw_namespace, 'BucketObjectsToCompare', cw_dimension_name, qs)
                        logger.warning("{} objects left to compare.".format(qs))
                    else:
                        break
            except KeyboardInterrupt:
                logger.warning("Exiting...")
                sys.exit(127)
            else:
                comparison_queue.join()
                logger.warning("Comparison queue finished.")

            for i in range(0, thread_count):
                logger.debug("Waiting for comparison thread {} to be finished.".format(i))
                try:
                    while True:
                        if th[i].is_alive():
                            time.sleep(1)
                        else:
                            break
                except KeyboardInterrupt:
                    logger.warning("Exiting...")
                    sys.exit(127)
                else:
                    th[i].join()
                    logger.info("Comparison thread {} finished.".format(i))
            logger.warning("Comparison of objects took {} seconds".format(round(time.time()-start)))

        # Check if there are any objects to copy.
        logger.info("Copy queue size: {}".format(copy_queue.qsize()))
        copy_queue_size = copy_queue.qsize()
        cw_put_queue_count(aws_session, cw_namespace, 'BucketObjectsToCopy', cw_dimension_name, copy_queue_size)
        if copy_queue_size > 0:
            # Check if copy_queue_size is less than THREAD_COUNT
            # If so set THREAD_COUNT to copy_queue_size
            if copy_queue_size < THREAD_COUNT:
                thread_count = copy_queue_size
            else:
                thread_count = THREAD_COUNT
            # Start copying S3 objects to destiantion bucket
            # Consume copy_queue until it is empty
            th = list()
            logger.warning("{} ojects to copy.".format(copy_queue_size))
            logger.warning("Starting copy task.")
            logger.info("Generating {} copy threads.".format(thread_count))
            start = time.time()
            for thread_num in range(0, thread_count):
                th.append(S3BackupRestore(
                    thread_num,
                    aws_session,
                    SOURCE_BUCKET,
                    DESTINATION_BUCKET,
                    cw_namespace,
                    cw_dimension_name))
                th[thread_num].daemon = True
                th[thread_num].start()
            logger.info("Waiting for copy queue to be finished.")
            try:
                while True:
                    if not copy_queue.empty():
                        time.sleep(15)
                        qs = copy_queue.qsize()
                        cw_put_queue_count(aws_session, cw_namespace, 'BucketObjectsToCopy', cw_dimension_name, qs)
                        logger.warning("{} objects left to copy".format(qs))
                    else:
                        break
            except KeyboardInterrupt:
                logger.warning("Exiting...")
                sys.exit(127)
            else:
                copy_queue.join()
                logger.warning("Copy queue finished.")

            for i in range(0, thread_count):
                logger.debug("Waiting for copy thread {} to be finished.".format(i))
                try:
                    while True:
                        if th[i].is_alive():
                            time.sleep(1)
                        else:
                            break
                except KeyboardInterrupt:
                    logger.warning("Exiting...")
                    sys.exit(127)
                else:
                    th[i].join()
                    logger.info("Copy thread number {} finished.".format(i))
            logger.warning("Copy task finished.")
        else:
            logger.warning("No objects to copy!")
        logger.warning("Copying of objects took {} seconds".format(round(time.time()-start)))

        # If script will be calle with flag --tag-deleted we will tag all newly deleted files
        # from source bucket as deleted in backup bucket
        if TAG_DELETED:
            try:
                # Add deleted keys to be marked to deleted keys queue
                keys_to_delete = set(dest_bucket_keys) - set(source_bucket_keys)
                logger.warning("{} keys to tag as deleted".format(len(keys_to_delete)))
                logger.debug("Keys to tag as deleted:\n{}".format(keys_to_delete))
                [deleted_keys_queue.put(key) for key in keys_to_delete]
                logger.debug("Deleted keys queue size {}.".format(deleted_keys_queue.qsize()))
                start = time.time()
            except:
                logger.exception("")

            # Check if there are any objects to copy.
            deleted_keys_size = deleted_keys_queue.qsize()
            cw_put_queue_count(aws_session, cw_namespace, 'BucketObjectsToTagAsDeleted', cw_dimension_name, deleted_keys_size)
            if deleted_keys_size > 0:
                # Check if deleted_keys_size is less than THREAD_COUNT
                # If so set THREAD_COUNT to deleted_keys_size
                if deleted_keys_size < THREAD_COUNT:
                    thread_count = deleted_keys_size
                else:
                    thread_count = THREAD_COUNT

                # Start tagging deleted S3 objects in destiantion bucket
                # Consume deleted_keys_queue until it is empty
                th = list()
                logger.warning("Starting tagging of deleted objects.")
                logger.info("Generating {} tagging thread(s).".format(thread_count))
                for thread_num in range(0, thread_count):
                    th.append(TagDeletedKeys(
                        thread_num,
                        aws_session,
                        DESTINATION_BUCKET,
                        cw_namespace,
                        cw_dimension_name))
                    th[thread_num].daemon = True
                    th[thread_num].start()
                logger.info("Waiting for deleted keys queue to be finished.")
                try:
                    while True:
                        if not deleted_keys_queue.empty():
                            time.sleep(15)
                            qs = deleted_keys_queue.qsize()
                            cw_put_queue_count(aws_session, cw_namespace, 'BucketObjectsToTagAsDeleted', cw_dimension_name, qs)
                            logger.warning("{} objects left to tag as deleted".format(deleted_keys_queue.qsize()))
                        else:
                            break
                except KeyboardInterrupt:
                    logger.warning("Exiting...")
                    sys.exit(127)
                else:
                    copy_queue.join()
                    logger.warning("Deleted keys queue finished.")

                for i in range(0, thread_count):
                    logger.debug("Waiting for tagging thread {} to be finished.".format(i))
                    try:
                        while True:
                            if th[i].is_alive():
                                time.sleep(1)
                            else:
                                break
                    except KeyboardInterrupt:
                        logger.warning("Exiting...")
                        sys.exit(127)
                    else:
                        th[i].join()
                        logger.info("Tagging thread number {} finished.".format(i))
                logger.warning("Tagging of deleted objects finished.")
            sum_deleted = 0
            while not deletion_count_queue.empty():
                try:
                    sum_deleted += int(deletion_count_queue.get())
                except:
                    logger.exception("")
                else:
                    deletion_count_queue.task_done()
            logger.info("{} keys marked as deleted.".format(sum_deleted))
            cw_put_queue_count(aws_session, cw_namespace, 'BucketObjectsMarkedAsDeleted', cw_dimension_name, deleted_keys_size)
            logger.warning("Tagging of objects took {} seconds".format(round(time.time()-start)))
