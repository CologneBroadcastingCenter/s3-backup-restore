import boto3
import threading
import queue
import logging
import time
import sys

from botocore.exceptions import ProfileNotFound

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(funcName)s line %(lineno)d: %(message)s"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
q = queue.Queue()
marked_as_deleted_queue = queue.Queue()


def get_s3_objects(aws_access_key_id, aws_secret_access_key, aws_session_token,
                   bucket, copy_num_objects=None):
    """Receives objects from S3 bucket.

    Uses boto3 and its resources to receive object keys from provided bucket.

    Args:
        aws_session (session): AWS session from boto3
        bucket (str): S3 bucket to list all object keys from
        copy_num_objects (int, optional): Defaults to None. Only for debugging
        purposes. If set to number > 0 it only returns number of objects.


    Returns:
        [list]: List consisting s3 bucket object keys.
    """

    try:
        aws_session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                            aws_secret_access_key=aws_secret_access_key,
                                            aws_session_token=aws_session_token)
    except Exception as exc:
        logger.exception("")

    logger.warning("Listing {} for objects.".format(bucket))
    keys = list()
    start = time.time()
    try:
        for key in aws_session.resource('s3').Bucket(bucket).objects.all():
            keys.append(key.key)

            if time.time() -1 > start:
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


class GetTaggedKeys(threading.Thread):

    def __init__(self, access_key, secret_key, session_token, bucket, timeout=30):
        threading.Thread.__init__(self)
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.bucket = bucket
        self.timeout = timeout
        self.daemon = True

    def run(self):
        global q
        global marked_as_deleted_queue

        try:
            self.session = boto3.session.Session(aws_access_key_id=self.access_key,
                                                 aws_secret_access_key=self.secret_key,
                                                 aws_session_token=self.session_token)
        except Exception as exc:
            logger.exception("")
        else:
            self.s3 = self.session.resource('s3')
            self.s3_client = self.s3.meta.client

            while not q.empty():
                try:
                    self.key = q.get(timeout=self.timeout)
                except queue.Empty as exc:
                    continue

                try:
                    self.resp = self.s3_client.get_object_tagging(Bucket=self.bucket,
                                                                  Key=self.key)

                    for self.ts in self.resp['TagSet']:
                        if 'Deleted' in self.ts['Key'] and 'True' in self.ts['Value']:
                            marked_as_deleted_queue.put(self.key)
                except Exception as exc:
                    logger.exception("")

if __name__ == '__main__':
    try:
        aws_session = boto3.session.Session(profile_name='rin-tvnow-backup-dev')
    except ProfileNotFound:
        aws_session = boto3.session.Session()

    creds = aws_session.get_credentials().get_frozen_credentials()
    bucket = 'tvnow-streaming-backup'

    all_keys = get_s3_objects(creds.access_key,
                              creds.secret_key,
                              creds.token,
                              bucket)

    [q.put(key) for key in all_keys]

    th = list()
    for i in range(200):
        th.append(GetTaggedKeys(
            creds.access_key,
            creds.secret_key,
            creds.token,
            bucket
            ))
        th[i].start()

    for i in range(200):
        th[i].join(timeout=300)

    print(marked_as_deleted_queue.qsize())
