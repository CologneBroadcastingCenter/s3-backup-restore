import boto3
import argparse
import logging
import queue
import sys
import threading
import time
from datetime import datetime

copy_queue = queue.Queue()
speed_queue = queue.Queue()
comparison_queue = queue.Queue()

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
                logger.error("To many connections open.")
            except:
                logger.exception("")
                break


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

            try:
                self.source_cl = self.s3.Object(self.source_bucket, self.key).content_length
                self.source_etag = self.s3.Object(self.source_bucket, self.key).e_tag

                self.dest_cl = self.s3.Object(self.dest_bucket, self.key).content_length
                self.dest_etag = self.s3.Object(self.dest_bucket, self.key).e_tag
            except ConnectionRefusedError as exc:
                logger.error("To many connections open.")
            except:
                logger.exception("")
                break
            else:
                if self.source_cl != self.dest_cl or self.source_etag != self.dest_etag:
                    print("Adding {} to queue".format(key))
                    copy_queue.put(key)
                comparison_queue.task_done()


def tag_deleted_keys(aws_session, backup_bucket, keys_to_tag):
    deleted_count = 0
    deleted_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    try:
        for key in keys_to_tag:
            deleted = True
            deleted_at = True

            resp = aws_session.meta.client.get_object_tagging(
                Bucket=backup_bucket,
                Key=key
            )

            for tag_key in resp['TagSet']:
                try:
                    if tag_key['Key'] == 'Deleted':
                        deleted = False
                    if tag_key['Key'] == 'DeletedAt':
                        deleted_at = False
                except KeyError:
                    logger.debug("Tag key 'Deleted' not set.")

            if deleted or deleted_at:
                kwargs = {
                    'Bucket': backup_bucket,
                    'Key': key,
                    'Tagging': {
                        'TagSet': [
                            {
                                'Key': 'Deleted',
                                'Value': 'True'
                            },
                            {
                                'Key': 'DeletedAt',
                                'Value': deleted_time
                            }
                        ]
                    }
                }
                logger.info("{} will be tagged as deleted.".format(key))
                resp = aws_session.meta.client.put_object_tagging(**kwargs)
                deleted_count += 1
        logger.info("Number of keys marked as deleted: {:d}".format(deleted_count))
    except:
        logger.exception("")


def get_s3_keys(aws_session, bucket):
    keys = list()

    try:
        keys = [key.key for key in aws_session.resource('s3').Bucket(bucket).objects.all()]
    except KeyboardInterrupt:
        print("Exiting...")
        sys.exit(127)
    except:
        logger.exception("")
        sys.exit(127)
    else:
        return keys


if __name__ == '__main__':
    try:
        if PROFILE:
            aws_session = boto3.session.Session(profile_name=PROFILE)
        else:
            aws_session = boto3.session.Session()
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

        # Start equality check of objects in S3
        # Puting objects not equal into copy_queue
        th = list()
        logger.info("Generating {} comparison threads.".format(THREAD_COUNT))
        for thread_num in range(0, THREAD_COUNT):
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

        for i in range(0, THREAD_COUNT):
            logger.info("Waiting for comparison thread {} to be finished.".format(i))
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
                THREAD_COUNT = copy_queue_size

            # Start copying S3 objects to destiantion bucket
            # Consume copy_queue until it is empty
            th = list()
            logger.info("Generating {} copy threads.".format(THREAD_COUNT))
            for thread_num in range(0, THREAD_COUNT):
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

            for i in range(0, THREAD_COUNT):
                logger.info("Waiting for copy thread {} to be finished.".format(i))
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
                deleted_keys = set(dest_bucket_keys) - set(source_bucket_keys)
                tag_deleted_keys(aws_session, DESTINATION_BUCKET, deleted_keys)
            except:
                logger.exception("")
