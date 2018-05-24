"""Receiving objects form bucket and returns them"""

import sys as sys
import time as time

from .config import Config
from .log import logger


def get_objects(bucket, config=None, objects_count=None):
    """Returns objects from bucket and returns them as a list

    Args:
        bucket (string): S3 bucket.
        config (Config, optional): Defaults to None.
        Configuration object.
        objects_count ([int], optional): Defaults to None.
                                         Amount of keys to return.

    Returns:
        [list]: List of S3 keys.
    """

    logger.info("Receive objects from {}.".format(bucket))
    keys = list()
    start = time.time()
    try:
        if config:
            session = config.boto3_session()
        else:
            session = Config.boto3_session()
    except:
        logger.exception("")
        sys.exit(127)

    try:
        for key in session.resource('s3').Bucket(bucket).objects.all():
            keys.append(key.key)

            if time.time() - 1 > start:
                logger.info("Received objects till now {}.".format(len(keys)))
                start = time.time()
            # Break condition to escape earlier thant complete bucket listing
            if objects_count and len(keys) >= objects_count:
                break

        logger.info("Summary of received objects {}.".format(len(keys)))
    except:
        logger.exception("")
        sys.exit(127)
    else:
        return keys


def delete_objects(bucket, config=None, with_versions=False):
    try:
        if config:
            session = config.boto3_session()
        else:
            session = Config.boto3_session()
        s3 = session.resource('s3')
    except:
        logger.exception("")
        sys.exit(127)

    try:
        s3.Bucket(bucket).objects.delete()
        if with_versions:
            s3.Bucket(bucket).object_versions.delete()
    except:
        logger.exception("")
        return False
    else:
        return True
