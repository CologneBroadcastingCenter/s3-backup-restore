#!/usr/bin/env python3
import argparse
import boto3
from botocore.exceptions import ClientError

parser = argparse.ArgumentParser()
parser.add_argument(
    '-p',
    '--profile',
    help='AWS Profile'
)
parser.add_argument(
    '--bucket',
    help='Buckets to use.',
    nargs='+'
)
parser.add_argument(
    '--enable',
    action='store_true',
    help='Enable Tranistioning to Glacier for next day.'
)
parser.add_argument(
    '--disable',
    action='store_true',
    help='Disable Transitioning to Glacier for next day.'
)
args = parser.parse_args()

PROFILE = args.profile
BUCKET = args.bucket
ENABLE = args.enable
DISABLE = args.disable


def transition_to_glacier(s3_res, bucket, status):
    if status:
        status = 'Enabled'
    else:
        status = 'Disabled'

    blc = s3_res.BucketLifecycle(bucket)
    blc.put(
        LifecycleConfiguration={
            'Rules': [
                {
                    'ID': status + 'TransitioningToGlacier',
                    'Status': status,
                    'Prefix': '',
                    'Transition': {
                        'Days': 0,
                        'StorageClass': 'GLACIER'
                    }
                }
            ]
        }
    )


def main():
    try:
        if PROFILE:
            session = boto3.session.Session(profile_name=PROFILE)
        else:
            session = boto3.session.Session()
        s3 = session.resource('s3')
    except ClientError as exc:
        print(exc.__str__)

    if ENABLE:
        for bucket in BUCKET:
            transition_to_glacier(s3, bucket, True)
            print("Enabled transitioning to Glacier for {}.".format(bucket))
    elif DISABLE:
        for bucket in BUCKET:
            transition_to_glacier(s3, bucket, False)
            print("Disabled transitioning to Glacier for {}.".format(bucket))
    else:
        print("Neither --enable nor disable chosen. Nothing happend.")


if __name__ == '__main__':
    main()
