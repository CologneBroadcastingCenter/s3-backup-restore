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
parser.add_argument('--delete', action='store_true', required=True)
parser.add_argument('--delete-versions', action='store_true')
parser.add_argument('-v', '--verbose', action='count')
cmd_args = parser.parse_args()

PROFILE = cmd_args.profile
BUCKET = cmd_args.bucket
DELETE = cmd_args.delete
VERBOSE = cmd_args.verbose
DELETE_VERSIONS = cmd_args.delete_versions


def delete_s3_objects(session, bucket, with_versions=False):
    s3_resource = session.resource('s3')
    try:
        if with_versions:
            s3_resource.Bucket(bucket).objects.delete()
            s3_resource.Bucket(bucket).object_versions.delete()
        else:
            s3_resource.Bucket(bucket).objects.delete()
    except:
        logger.exception("")
        return False
    else:
        return True


if __name__ == '__main__':
    if VERBOSE:
        logger.setLevel(logging.DEBUG)

    if PROFILE:
        session = boto3.session.Session(profile_name=PROFILE)
    else:
        session = boto3.session.Session()

    if DELETE and DELETE_VERSIONS:
        answere = input("Are you sure you want to delete ALL keys and its versions in {}?[Y/N]: ".format(BUCKET))
    elif DELETE:
        answere = input("Are you sure you want to delete ALL keys in {}?[Y/N]: ".format(BUCKET))

    if answere.upper() == 'Y':
        ret = delete_s3_objects(session, BUCKET, with_versions=DELETE_VERSIONS)
        if ret:
            print("Deletion completed!")
    else:
        print("Aborting!")
