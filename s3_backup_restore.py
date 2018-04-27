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
parser.add_argument('--dry-run', action='store_true', help='Simulate what will happen.')
cmd_args = parser.parse_args()

PROFILE = cmd_args.profile
SOURCE_BUCKET = cmd_args.source_bucket
DESTINATION_BUCKET = cmd_args.destination_bucket
PREFIX = cmd_args.prefix
TAG_DELETED = cmd_args.tag_deleted
THREAD_COUNT = cmd_args.thread_count
VERBOSE = cmd_args.verbose
DRY_RUN = cmd_args.dry_run


if VERBOSE and VERBOSE > 0:
    logger.setLevel(logging.DEBUG)


class S3GetDifferentKeys(threading.Thread):
    pass


class S3BackupRestore(threading.Thread):
    def __init__(self, thread_number, aws_session, source_bucket, destination_bucket):
        threading.Thread.__init__(self)
        self.thread_number = thread_number
        self.aws_session = aws_session
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket

    def run(self):
        global copy_queue
        global speed_queue
        self.s3 = self.aws_session.resource('s3')

        while not copy_queue.empty():
            self.key = copy_queue.get()
            content_length = 0
            try:
                # Receiving content_length ob Object in Bucket
                self.source_obj = self.s3.Object(self.source_bucket, self.key)
                content_length = self.source_obj.content_length
            except:
                logger.debug('Content_length not available. Setting to 0.')

            # Preparing copy task
            self.dest_obj = self.s3.Object(self.destination_bucket, self.key)
            logger.info("Thread number {} copying key: {}".format(self.thread_number, self.key))
            try:
                start = time.time()

                self.dest_obj.copy(
                    {
                        'Bucket': self.source_bucket,
                        'Key': self.key
                    }
                )
                copy_queue.task_done()
                stop = time.time()
            except ConnectionRefusedError as exc:
                logger.error("To many connections open.")
            except:
                logger.exception("")
                break
            else:
                speed_queue.put((content_length, float(stop - start)))


def get_s3_keys(aws_session, bucket):
    keys = list()

    try:
        for key in aws_session.resource('s3').Bucket(bucket).objects.all():
            keys.append(key.key)
    except:
        logger.exception("")
        sys.exit(127)
    else:
        return keys


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

if __name__ == '__main__':
    try:
        if PROFILE:
            aws_session = boto3.session.Session(profile_name=PROFILE)
        else:
            aws_session = boto3.session.Session()
        s3_client = aws_session.client('s3')
    except:
        logger.exception("")
        sys.exit(127)
    else:
        # Getting S3 keys from source bucket
        logger.info("List S3 Keys from {}".format(SOURCE_BUCKET))
        source_bucket_keys = get_s3_keys(aws_session, SOURCE_BUCKET)
        logger.info("Number of keys in {}: {}".format(SOURCE_BUCKET, len(source_bucket_keys)))

        # Getting S3 keys from source bucket
        logger.info("List S3 Keys from {}".format(DESTINATION_BUCKET))
        dest_bucket_keys = get_s3_keys(aws_session, DESTINATION_BUCKET)
        logger.info("Number of keys in {}: {}".format(DESTINATION_BUCKET, len(dest_bucket_keys)))

        # Adding source bucket keys to queue to make it thread safe
        for key in source_bucket_keys:
            copy_queue.put(key)

        th = list()
        logger.info("Generating {} threads.".format(THREAD_COUNT))
        for thread_num in range(0, THREAD_COUNT):
            th.append(S3BackupRestore(thread_num, aws_session, SOURCE_BUCKET, DESTINATION_BUCKET))
            th[thread_num].daemon = True
            th[thread_num].start()
        logger.debug("Waiting for queue to be finished.")
        copy_queue.join()
        logger.debug("Queue finished.")

        for i in range(0, THREAD_COUNT):
            logger.debug("Waiting for thread number {} to be finished.".format(i))
            th[i].join()
            logger.debug("Thread number {} finished.".format(i))

        # If script will be calle with flag --tag-deleted we will tag all newly deleted files
        # from source bucket as deleted in backup bucket
        if TAG_DELETED:
            try:
                deleted_keys = set(dest_bucket_keys) - set(source_bucket_keys)
                tag_deleted_keys(s3_client, DESTINATION_BUCKET, deleted_keys)
            except:
                logger.exception("")

        byte = 0
        time = 0
        while not speed_queue.empty():
            obj = speed_queue.get()
            byte += obj[0]
            time += obj[1]
        print("Transmissioned Bytes: {:.2f}".format(byte))
        print("Total time s: {}".format(time))
