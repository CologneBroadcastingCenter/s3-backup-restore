import boto3
import logging
import argparse

logging.basicConfig(
    level=logging.WARNING
)
logger = logging.getLogger(__name__)


parser = argparse.ArgumentParser()
parser.add_argument('-p', '--profile')
parser.add_argument('-b', '--bucket')
parser.add_argument('--delete', action='store_true')
parser.add_argument('-v', '--verbose', action='count')
cmd_args = parser.parse_args()

PROFILE = cmd_args.profile
BUCKET = cmd_args.bucket
DELETE = cmd_args.delete
VERBOSE = cmd_args.verbose


def get_s3_objects(session, bucket):
    keys = list()
    s3_resource = session.resource('s3')
    try:
        for key in s3_resource.Bucket(bucket).objects.all():
            keys.append(key.key)
    except:
        logger.exception("")
        sys.exit(127)
    else:
        return keys


def delete_s3_objects(session, bucket, keys_to_delete):
    s3_resource = session.resource('s3')
    deleted = 0
    while len(keys_to_delete):
        key = keys_to_delete.pop()
        try:
            s3_resource.Object(bucket, key).delete()
            deleted += 1
        except:
            logger.exception("")
            keys_to_delete.append(key)

    return deleted


if __name__ == '__main__':
    if VERBOSE:
        logger.setLevel(logging.DEBUG)

    if PROFILE:
        session = boto3.session.Session(profile_name=PROFILE)
    else:
        session = boto3.session.Session()

    keys_to_delete = get_s3_objects(session, BUCKET)
    logger.debug("Keys to be deleted: {}".format(keys_to_delete))

    if DELETE:
        if keys_to_delete:
            answere = input("Are you sure you want to delete ALL keys in {}?[Y/N]: ".format(BUCKET))
            if answere.upper() == 'Y':
                ret = delete_s3_objects(session, BUCKET, keys_to_delete)
                print("Return: {}".format(ret))
            else:
                print("Aborting!")
        else:
            print("No keys in {}!".format(BUCKET))
    else:
        logger.warning("[DRYRUN] following keys would be deleted.\n{}".format(keys_to_delete))
