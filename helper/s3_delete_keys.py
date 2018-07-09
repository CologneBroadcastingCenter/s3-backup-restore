import boto3
import logging
import argparse

logging.basicConfig(
    level=logging.WARNING
)
logger = logging.getLogger(__name__)


parser = argparse.ArgumentParser()
parser.add_argument('-p', '--profile')
parser.add_argument('-b', '--buckets', nargs='+')
parser.add_argument(
    '--delete-objects',
    action='store_true',
    required=True)
parser.add_argument(
    '--delete-object-versions',
    action='store_true')
parser.add_argument(
    '-v',
    '--verbose',
    action='count')
parser.add_argument(
    '--delete-bucket',
    action='store_true',
    help='Delete the bucket. Be aware that the bucket must be empty.')
cmd_args = parser.parse_args()

PROFILE = cmd_args.profile
BUCKETS = cmd_args.buckets
DELETE_OBJECTS = cmd_args.delete_objects
DELETE_OBJECT_VERSIONS = cmd_args.delete_object_versions
DELETE_BUCKET = cmd_args.delete_bucket
VERBOSE = cmd_args.verbose


def delete_s3_objects(session, bucket, with_versions=False):
    s3_resource = session.resource('s3')
    try:
        if with_versions:
            s3_resource.Bucket(bucket).objects.delete()
            s3_resource.Bucket(bucket).object_versions.delete()
        else:
            s3_resource.Bucket(bucket).objects.delete()
    except Exception as exc:
        if exc.args[0].startswith('An error occurred (NoSuchBucket)'):
            logger.warning("Bucket {} does not exists.".format(bucket))
        else:
            print(exc.args[0])
            # logger.exception("")
    else:
        return True


def delete_s3_bucket(session, bucket):
    try:
        bucket = session.resource('s3').Bucket(bucket)
        bucket.delete()
    except Exception as exc:
        if exc.args[0] in 'NoSuchBucket':
            logger.warning("Bucket {} does not exists.".format(bucket))
        else:
            logger.exception("")


if __name__ == '__main__':
    if VERBOSE:
        logger.setLevel(logging.DEBUG)

    if PROFILE:
        session = boto3.session.Session(profile_name=PROFILE)
    else:
        session = boto3.session.Session()

    for bucket in BUCKETS:
        if DELETE_OBJECTS and DELETE_OBJECT_VERSIONS:
            answere = input("Are you sure you want to delete ALL keys and its "
                            "versions in {}? Y means Yes for all.[y/Y/N]: "
                            .format(bucket))
        elif DELETE_OBJECTS:
            answere = input("Are you sure you want to delete ALL keys in "
                            "{}? Y means Yes for all.[y/Y/N]: ".format(bucket))

        if answere == 'Y':
            for i in range(len(BUCKETS)):
                ret = delete_s3_objects(
                    session,
                    BUCKETS[i],
                    with_versions=DELETE_OBJECT_VERSIONS)
                if DELETE_BUCKET:
                    delete_s3_bucket(session, BUCKETS[i])
                if ret:
                    print("Deletion completed of {}!".format(BUCKETS[i]))
            else:
                break
        elif answere == 'y':
            ret = delete_s3_objects(
                session,
                bucket,
                with_versions=DELETE_OBJECT_VERSIONS)
            if DELETE_BUCKET:
                delete_s3_bucket(session, BUCKETS[i])
            if ret:
                print("Deletion completed of {}!".format(bucket))
        else:
            print("Aborting!")
