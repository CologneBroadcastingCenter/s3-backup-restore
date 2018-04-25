import boto3
import argparse
import logging
import queue
import sys
import threading
from datetime import datetime

copy_queue = queue.Queue()

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

if cmd_args.verbose and cmd_args.verbose > 0:
    logger.setLevel(logging.DEBUG)


class S3GetDifferentKeys(threading.Thread):



class S3BackupRestore(threading.Thread):
    def __init__(self, thread_number, aws_session, source_bucket, destination_bucket):
        threading.Thread.__init__(self)
        self.thread_number = thread_number
        self.aws_session = aws_session
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket

    def run(self):
        global copy_queue
        self.resource = self.aws_session.resource('s3')
        self.dest_bucket = self.resource.Bucket(self.destination_bucket)

        while not copy_queue.empty():
            self.key = copy_queue.get()
            self.dest_obj = self.dest_bucket.Object(self.key)

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


def get_s3_keys(s3_client, bucket, prefix=''):
    kwargs = {'Bucket': bucket, 'Prefix': prefix}
    keys = list()

    while True:
        # The S3 API response is a blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3_client.list_objects_v2(**kwargs)
        try:
            for key in resp['Contents']:
                keys.append(key['Key'])
        except KeyError as exc:
            logger.error("No content in bucket {}.".format(bucket))
            return keys
        else:
            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                return keys
                break


def tag_deleted_keys(s3_client, backup_bucket, keys_to_tag):
    deleted_count = 0
    deleted_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    try:
        for key in keys_to_tag:
            deleted = True
            deleted_at = True

            resp = s3_client.get_object_tagging(
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

            logger.debug(deleted)
            logger.debug(deleted_at)
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
                logger.info("Key that will be tagged as deleted: {}".format(key))
                resp = s3_client.put_object_tagging(**kwargs)
                deleted_count += 1
        logger.info("Number of newly keys to be marked as deleted: {:d}".format(deleted_count))
    except:
        logger.exception("")

if __name__ == '__main__':
    try:
        if cmd_args.profile:
            aws_session = boto3.session.Session(profile_name=cmd_args.profile)
        else:
            aws_session = boto3.session.Session()
        s3_client = aws_session.client('s3')
    except:
        logger.exception("")
        sys.exit(127)
    else:
        # Getting S3 keys from source bucket
        logging.info("List S3 Keys from {}".format(cmd_args.source_bucket))
        source_bucket_keys = get_s3_keys(s3_client, cmd_args.source_bucket, cmd_args.prefix)
        logger.debug("Keys from {} bucket:\n{}".format(cmd_args.source_bucket, source_bucket_keys))

        # Getting S3 keys from source bucket
        logging.info("List S3 Keys from {}".format(cmd_args.destination_bucket))
        dest_bucket_keys = get_s3_keys(s3_client, cmd_args.destination_bucket, cmd_args.prefix)
        logger.debug("Keys from {} bucket:\n{}".format(cmd_args.destination_bucket, dest_bucket_keys))

        # Adding source bucket keys to queue to make it thread safe
        for key in source_bucket_keys:
            copy_queue.put(key)

        th = list()
        logger.info("Generating {} threads.".format(cmd_args.thread_count))
        for i in range(0, cmd_args.thread_count):
            th.append(S3BackupRestore(i, aws_session, cmd_args.source_bucket, cmd_args.destination_bucket))
            th[i].daemon = True
            th[i].start()
        copy_queue.join()

        for i in range(0, cmd_args.thread_count):
            th[i].join()

        # If script will be calle with flag --tag-deleted we will tag all newly deleted files
        # from source bucket as deleted in backup bucket
        if cmd_args.tag_deleted:
            try:
                deleted_keys = set(dest_bucket_keys) - set(source_bucket_keys)
                tag_deleted_keys(s3_client, cmd_args.destination_bucket, deleted_keys)
            except:
                logger.exception("")
