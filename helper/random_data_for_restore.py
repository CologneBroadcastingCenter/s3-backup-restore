#!/usr/bin/env python3
import argparse
import boto3
import string
import sys
from os import urandom
from random import choices, randint
from botocore.exceptions import ClientError

parser = argparse.ArgumentParser()
parser.add_argument(
    '-p',
    '--profile',
    help='AWS Profile'
)
parser.add_argument(
    '--bucket',
    help='Bucket to use.'
)
parser.add_argument(
    '--number-of-buckets',
    help='Number of buckets to create. '
         'Each bucket name will be appended with a number.',
    default=1,
    type=int
)
parser.add_argument(
    '--object-count',
    help='Number of objects to create and update.',
    default=100,
    metavar='N',
    type=int
)
parser.add_argument(
    '--max-object-size',
    help='Size in Mb of each object that will be created.',
    default=1,
    metavar='N',
    type=int
)
parser.add_argument(
    '--tag-random-objects',
    action='store_true',
    help='Tag random objects as deleted.'
)
parser.add_argument(
    '--percent',
    type=int,
    default=50,
    help='Percent of tagged objects.'
)
args = parser.parse_args()

PROFILE = args.profile
BUCKET = args.bucket
NUMBER_OF_BUCKETS = args.number_of_buckets
OBJECT_COUNT = args.object_count
MAX_OBJECT_SIZE = args.max_object_size
TAG_RANDOM_OBJECTS = args.tag_random_objects
PERCENT = args.percent


def create_bucket(boto3_session, bucket_name):
    s3 = boto3_session.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.create(
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-central-1'
        }
    )


def upload_object(boto3_session, bucket, byte_object):
    s3 = boto3_session.resource('s3')
    key = ''.join(choices(string.ascii_lowercase[:6] + string.digits, k=50))
    obj = s3.Object(bucket, key)
    obj.put(
        Body=byte_object
    )
    return key


def tag_object_as_deleted(boto3_session, bucket, key):
    s3 = boto3_session.resource('s3')
    s3_client = s3.meta.client

    s3_client.put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={
            'TagSet': [
                {
                    'Key': 'Deleted',
                    'Value': 'True'
                }
            ]
        }
    )


def create_objects(obj_size):
    return urandom(randint(1024**2, obj_size * 1024**2))


if __name__ == '__main__':
    if PROFILE:
        session = boto3.session.Session(profile_name=PROFILE)
    else:
        session = boto3.session.Session()
    s3 = session.resource('s3')

    answere = ""
    for num in range(NUMBER_OF_BUCKETS):
        try:
            bucket_name = BUCKET + "-" + str(num)
            s3.meta.client.head_bucket(Bucket=bucket_name)
        except ClientError as exc:
            if 'Forbidden' in exc.args[0]:
                print("You do not have access to {}.".format(bucket_name))
            elif 'Not Found' in exc.args[0]:
                print("There is no bucket {}".format(bucket_name))
                if answere == 'y' or answere == "":
                    answere = input("Do you want to create {}, "
                                    "Y means yes to all?([Y,y]/n)"
                                    .format(bucket_name))
                if 'Y' in answere.upper():
                    print("Create test Bucket {}".format(bucket_name))
                    create_bucket(session, bucket_name)
                else:
                    print("Exiting...")
                    sys.exit(127)
            else:
                print("{}".format(exc.args))

        count_obj = 0
        count_deleted_obj = 0
        count = 0
        for i in range(OBJECT_COUNT):
            s3_byte_obj = create_objects(MAX_OBJECT_SIZE)
            key = upload_object(session, bucket_name, s3_byte_obj)

            if TAG_RANDOM_OBJECTS:
                if round((PERCENT * count), 1) >= 100:
                    tag_object_as_deleted(session, bucket_name, key)
                    print("{} tagged as deleted.".format(key))
                    count = 0
                    count_deleted_obj += 1
                count += 1
            count_obj += 1

        print("Conclusion:")
        print("{} objects in {}".format(count_obj, bucket_name))
        print("{} objects tagged as deleted.".format(count_deleted_obj))
