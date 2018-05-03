import boto3
import argparse
import logging
import queue
import sys
import threading
import time
import uuid
from datetime import datetime
from botocore.exceptions import ClientError, ParamValidationError

copy_queue = queue.Queue()
speed_queue = queue.Queue()
comparison_queue = queue.Queue()
deleted_keys_queue = queue.Queue()
deletion_count_queue = queue.Queue()

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(funcName)s line %(lineno)d: %(message)s"
)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument('--profile', '-p', help='AWS profile name configure in aws config files.')
parser.add_argument('--source-bucket', '-s', help='Source Bucket to sync diff from.', required=True)
parser.add_argument('--destination-bucket', '-d', help='Destination Bucket to sync to.', required=True)
parser.add_argument('--prefix', default='', help='Prefix to list objects from and to sync to.')
parser.add_argument('--tag-deleted', action='store_true', help='Tag all objects that are deleted in source bucket but still present in backup bucket.')
parser.add_argument('--thread-count', default=10, metavar='N', type=int, help='Starting count for threads.')
parser.add_argument('--verbose', '-v', action='count')
cmd_args = parser.parse_args()

PROFILE = cmd_args.profile
SOURCE_BUCKET = cmd_args.source_bucket
DESTINATION_BUCKET = cmd_args.destination_bucket
PREFIX = cmd_args.prefix
TAG_DELETED = cmd_args.tag_deleted
THREAD_COUNT = cmd_args.thread_count
VERBOSE = cmd_args.verbose


if VERBOSE and VERBOSE == 1:
    logger.setLevel(logging.INFO)
elif VERBOSE and VERBOSE == 2:
    logger.setLevel(logging.DEBUG)


class S3BackupRestore(threading.Thread):
    def __init__(self, thread_number, aws_session, source_bucket, destination_bucket):
        threading.Thread.__init__(self)
        self.thread_number = thread_number
        self.aws_session = aws_session
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket

    def run(self):
        global copy_queue
        self.s3 = self.aws_session.resource('s3')

        while not copy_queue.empty():
            self.uuid = uuid.uuid4()
            self.key = copy_queue.get()

            # Preparing copy task
            self.dest_obj = self.s3.Object(self.destination_bucket, self.key)
            logger.info("Thread number {} copying key: {}".format(self.thread_number, self.key))
            try:
                self.dest_obj.copy(
                    {
                        'Bucket': self.source_bucket,
                        'Key': self.key
                    }
                )
                copy_queue.task_done()
            except ConnectionRefusedError as exc:
                logger.error("""To many connections open.
                Put {} back to queue.
                UUDI: {}""".format(self.key, self.uuid))
                copy_queue.put(self.key)
            except:
                logger.exception("""Unhandeld exception occured.
                Put {} back to queue.
                UUDI: {}""".format(self.key, self.uuid))
                copy_queue.put(self.key)


class CompareKeysClAndETag(threading.Thread):
    def __init__(self, thread_number, aws_session, source_bucket, dest_bucket):
        threading.Thread.__init__(self)
        self.thread_number = thread_number
        self.aws_session = aws_session
        self.source_bucket = source_bucket
        self.dest_bucket = dest_bucket

    def run(self):
        global copy_queue
        global comparison_queue
        try:
            self.s3 = self.aws_session.resource('s3')
        except:
            logger.exception("")
        while not comparison_queue.empty():
            self.key = comparison_queue.get()
            self.uuid = uuid.uuid4()

            try:
                self.source_cl = self.s3.Object(self.source_bucket, self.key).content_length
                self.source_etag = self.s3.Object(self.source_bucket, self.key).e_tag

                self.dest_cl = self.s3.Object(self.dest_bucket, self.key).content_length
                self.dest_etag = self.s3.Object(self.dest_bucket, self.key).e_tag
            except ConnectionRefusedError as exc:
                logger.error(""""To many connections open.
                Put {} back to queue.
                UUDI: {}""".format(self.key, self.uuid))
                comparison_queue.put(self.key)
            except:
                logger.exception("""Unhandeld exception occured.
                Put {} back to queue.
                UUDI: {}""".format(self.key, self.uuid))
                comparison_queue.put(self.key)
            else:
                if self.source_cl != self.dest_cl or self.source_etag != self.dest_etag:
                    logger.info("Adding {} to queue".format(self.key))
                    copy_queue.put(self.key)
                comparison_queue.task_done()


class TagDeletedKeys(threading.Thread):
    def __init__(self, thread_number, aws_session, destination_bucket):
        threading.Thread.__init__(self)
        self.thread_number = thread_number
        self.aws_session = aws_session
        self.destination_bucket = destination_bucket

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
            self.uuid = uuid.uuid4()

            try:
                logger.debug("Getting tagging information from {}".format(self.key))
                self.resp = self.s3.meta.client.get_object_tagging(
                    Bucket=self.destination_bucket,
                    Key=self.key
                )
            except ConnectionRefusedError:
                logger.error(""""To many connections open.
                Put {} back to queue.
                UUDI: {}""".format(self.key, self.uuid))
                deleted_keys_queue.put(self.key)
                continue
            except:
                logger.exception("""Unhandeld exception occured.
                Put {} back to queue.
                UUDI: {}""".format(self.key, self.uuid))
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


def get_s3_keys(aws_session, bucket):
    logger.info("Listing {} for objects.".format(bucket))
    keys = list()
    try:
        keys = [key.key for key in aws_session.resource('s3').Bucket(bucket).objects.all()]
    except:
        logger.exception("")
        sys.exit(127)
    else:
        return keys


if __name__ == '__main__':
    try:
        if PROFILE:
            aws_session = boto3.session.Session(profile_name=PROFILE)
            cred = aws_session.get_credentials().get_frozen_credentials()
        else:
            aws_session = boto3.session.Session()
    except KeyboardInterrupt:
        print("Exiting...")
        sys.exit(127)
    except ClientError:
        print("Exiting program. Wrong MFA token!")
        sys.exit(127)
    except ParamValidationError:
        print("Exiting program. Empty MFA token!")
        sys.exit(127)
    except:
        logger.exception("")
        sys.exit(127)
    else:
        # Getting S3 keys from source bucket
        logger.debug("List S3 Keys from {}".format(SOURCE_BUCKET))
        source_bucket_keys = get_s3_keys(aws_session, SOURCE_BUCKET)
        logger.info("Number of keys in {}: {}".format(SOURCE_BUCKET, len(source_bucket_keys)))

        # Getting S3 keys from source bucket
        logger.debug("List S3 Keys from {}".format(DESTINATION_BUCKET))
        dest_bucket_keys = get_s3_keys(aws_session, DESTINATION_BUCKET)
        logger.info("Number of keys in {}: {}".format(DESTINATION_BUCKET, len(dest_bucket_keys)))

        # Sending keys to queue if they are not in Destination Bucket
        keys_not_in_dest_bucket = set(source_bucket_keys) - set(dest_bucket_keys)
        [copy_queue.put(key) for key in keys_not_in_dest_bucket]
        logger.info("{} key(s) not in destination Bucket.".format(len(keys_not_in_dest_bucket)))
        logger.debug("Keys not in destination Bucket: {}".format(keys_not_in_dest_bucket))
        logger.info("Copy queue size: {}".format(copy_queue.qsize()))

        # Check all keys existing in destination bucket and source bucket
        keys_to_check = set(source_bucket_keys) & set(dest_bucket_keys)
        [comparison_queue.put(key) for key in keys_to_check]
        logger.info("{} keys to check for equality.".format(len(keys_to_check)))
        logger.debug("Keys not in destination Bucket: {}".format(keys_to_check))

        # Check if there are any objects to compare.
        comparison_queue_size = comparison_queue.qsize()
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
            logger.info("Generating {} comparison threads.".format(thread_count))
            for thread_num in range(0, thread_count):
                th.append(CompareKeysClAndETag(thread_num, aws_session, SOURCE_BUCKET, DESTINATION_BUCKET))
                th[thread_num].daemon = True
                th[thread_num].start()

            logger.info("Waiting for comparison queue to be finished.")
            try:
                while True:
                    if not comparison_queue.empty():
                        time.sleep(500/1000)
                    else:
                        break
            except KeyboardInterrupt:
                print("Exiting...")
                sys.exit(127)
            else:
                comparison_queue.join()
                logger.info("Comparison queue finished.")

            for i in range(0, thread_count):
                logger.debug("Waiting for comparison thread {} to be finished.".format(i))
                try:
                    while True:
                        if th[i].is_alive():
                            time.sleep(500/1000)
                        else:
                            break
                except KeyboardInterrupt:
                    print("Exiting...")
                    sys.exit(127)
                else:
                    th[i].join()
                    logger.info("Comparison thread {} finished.".format(i))

        # Check if there are any objects to copy.
        copy_queue_size = copy_queue.qsize()
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
            logger.info("Generating {} copy threads.".format(thread_count))
            for thread_num in range(0, thread_count):
                th.append(S3BackupRestore(thread_num, aws_session, SOURCE_BUCKET, DESTINATION_BUCKET))
                th[thread_num].daemon = True
                th[thread_num].start()
            logger.info("Waiting for copy queue to be finished.")
            try:
                while True:
                    if not copy_queue.empty():
                        time.sleep(500/1000)
                    else:
                        break
            except KeyboardInterrupt:
                print("Exiting...")
                sys.exit(127)
            else:
                copy_queue.join()
                logger.info("Copy queue finished.")

            for i in range(0, thread_count):
                logger.debug("Waiting for copy thread {} to be finished.".format(i))
                try:
                    while True:
                        if th[i].is_alive():
                            time.sleep(500/1000)
                        else:
                            break
                except KeyboardInterrupt:
                    print("Exiting...")
                    sys.exit(127)
                else:
                    th[i].join()
                    logger.info("Copy thread number {} finished.".format(i))
        else:
            logger.info("No objects to copy!")

        # If script will be calle with flag --tag-deleted we will tag all newly deleted files
        # from source bucket as deleted in backup bucket
        if TAG_DELETED:
            try:
                # Add deleted keys to be marked to deleted keys queue
                keys_to_delete = set(dest_bucket_keys) - set(source_bucket_keys)
                logger.debug("Keys to tag as deleted:\n{}".format(keys_to_delete))
                [deleted_keys_queue.put(key) for key in keys_to_delete]
            except:
                logger.exception("")

            # Check if there are any objects to copy.
            deleted_keys_size = deleted_keys_queue.qsize()
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
                logger.info("Generating {} tagging thread(s).".format(thread_count))
                for thread_num in range(0, thread_count):
                    th.append(TagDeletedKeys(thread_num, aws_session, DESTINATION_BUCKET))
                    th[thread_num].daemon = True
                    th[thread_num].start()
                logger.info("Waiting for deleted keys queue to be finished.")
                try:
                    while True:
                        if not deleted_keys_queue.empty():
                            time.sleep(500/1000)
                        else:
                            break
                except KeyboardInterrupt:
                    print("Exiting...")
                    sys.exit(127)
                else:
                    copy_queue.join()
                    logger.info("Deleted keys queue finished.")

                for i in range(0, thread_count):
                    logger.debug("Waiting for tagging thread {} to be finished.".format(i))
                    try:
                        while True:
                            if th[i].is_alive():
                                time.sleep(500/1000)
                            else:
                                break
                    except KeyboardInterrupt:
                        print("Exiting...")
                        sys.exit(127)
                    else:
                        th[i].join()
                        logger.info("Tagging thread number {} finished.".format(i))

            sum_deleted = 0
            while not deletion_count_queue.empty():
                try:
                    sum_deleted += int(deletion_count_queue.get())
                except:
                    logger.exception("")
                else:
                    deletion_count_queue.task_done()
            logger.info("{} keys marked as deleted.".format(sum_deleted))
