import boto3
import argparse
import logging
import queue
import sys
import threading

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s [%(levelname)s] %(funcName)s line %(lineno)d: %(message)s"
)
logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument('--profile-name', '-p', default='default', help='AWS profile name configure in aws config files.')
parser.add_argument('--source-bucket', '-s', help='Source Bucket to sync diff from.', required=True)
parser.add_argument('--destination-bucket', '-d', help='Destination Bucket to sync to.', required=True)
parser.add_argument('--prefix', default='', help='Prefix to list objects from and to sync to.')
parser.add_argument('--threads-count', default=100, help='Starting count for threads.')
parser.add_argument('--verbose', '-v', action='count')
parser.add_argument('--dry-run', action='store_true', help='Simulate what will happen.')
cmd_args = parser.parse_args()

if cmd_args.verbose and cmd_args.verbose > 0:
    logger.setLevel(logging.DEBUG)


def get_bucket_keys(s3_client, bucket, prefix=""):
    continuation_token = None
    keys = list()
    while True:
        res = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            ContinuationToken=continuation_token
            )
        for key in res['Contents']:
            keys.append(key['Key'])
        try:
            continuation_token = res['NextContinuationToken']
        except KeyError as exc:
            logger.info("All keys are listed!")

    return keys


def get_changed_keys(s3_client, bucket, prefix=""):


try:
    session = boto3.session.Session(profile_name=cmd_args.profile_name)
    s3_client = session.client('s3')
except:
    logger.debug("", exc_info=True)
    sys.exit(127)
else:
    source_keys = get_bucket_keys(s3_client, cmd_args.source_bucket, cmd_args.prefix)
    dest_keys = get_bucket_keys(s3_client, cmd_args.destination_bucket, cmd_args.prefix)

    diff_key = set(source_keys) - set(dest_keys)

    q = queue.Queue()
    if len(diff_key) > 0:
        logger.info("{0} objects not in destination bucket.\nAdding {0} keys to queue.".format(len(diff_key)))
        for k in diff_key:
            q.put(k)
    else:
        logger.info("No Objects to sync. Exiting!")
        sys.exit(0)
############################################################
    for key in diff_key:
        resource = session.resource('s3')
        dest_bucket = resource.Bucket(cmd_args.destination_bucket)
        dest_obj = dest_bucket.Object(key)

        if not cmd_args.dry_run:
            logger.info("Copying key: {}".format(key))
            dest_obj.copy(
                {
                    'Bucket': cmd_args.source_bucket,
                    'Key': key
                }
            )
        else:
            logger.debug("[DRY-RUN] Copying key: {}".format(key))
