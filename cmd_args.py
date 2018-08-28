import argparse
import logging
import os

#
# Arguments
#
# -----------------------------------------------------------------------------
# env_or_required_arg
# helper function to supply input parameters via environment variables or
# arguments
# -----------------------------------------------------------------------------


def env_or_required_arg(key, required=True, default=None):
    value = os.environ.get(key)
    # Checking verbosity level
    if value:
        if key in 'VERBOSE':
            if 'WARNING' in value:
                value = logging.WARNING
            elif 'INFO' in value:
                value = logging.INFO
            elif 'DEBUG' in value:
                value = logging.DEBUG

        return {'default': value}
    elif default:
        return {
            'required': False,
            'default': default
        }
    else:
        return {'required': required}


parser = argparse.ArgumentParser(add_help=False)
parser.add_argument(
    '--source-bucket',
    '-s',
    help="Source Bucket to sync diff from. "
         "(env: S3_SRC_BUCKET)",
    **env_or_required_arg('S3_SRC_BUCKET'))
parser.add_argument(
    '--destination-bucket',
    '-d',
    help="Destination Bucket to sync to. "
         "(env: S3_DEST_BUCKET)",
    **env_or_required_arg('S3_DEST_BUCKET'))
parser.add_argument(
    '--prefix',
    help="Prefix to list objects from and to sync to. "
         "(env: PREFIX)",
    **env_or_required_arg('PREFIX', required=False))
parser.add_argument(
    '-a',
    '--all',
    help="Copies all objects. "
         "It doesn't compare, tag nor check objects in destination."
         "Use this option only if you want a full copy of your backup! "
         "(env: ALL)",
    action='store_true',
    **env_or_required_arg('ALL', required=False)
)
parser.add_argument(
    '--thread-count-per-proc',
    metavar='N',
    type=int,
    help="Number of threads started in each process. "
         "(env: THREAD_COUNT_PER_PROC)",
    **env_or_required_arg('THREAD_COUNT_PER_PROC', required=False))
parser.add_argument(
    '--timeout',
    type=int,
    metavar='s',
    help="Sets the timeout to finish each task before skipping. "
         "(env: TIMEOUT)",
    **env_or_required_arg('TIMEOUT', default=30))
parser.add_argument(
    '--profile',
    '-p',
    help="AWS profile to use. "
         "(environment: AWS_PROFILE)",
    **env_or_required_arg('AWS_PROFILE', required=False)
)
parser.add_argument(
    '--region',
    help="The AWS region to use. "
         "(environment: AWS_REGION, default: eu-central-1)",
    **env_or_required_arg('AWS_REGION', default='eu-central-1'))
parser.add_argument(
    '--cloudwatch-dimension-name',
    help="Cloudwatch Dimension name to use to publish metrics to. "
         "(env: CW_DIMENSION_NAME)",
    **env_or_required_arg('CW_DIMENSION_NAME', default='Dev'))
parser.add_argument(
    '--verbose', '-v',
    action='count',
    help="Increases the output of script. WARNING|INFO|DEBUG "
         "(environment: VERBOSE)",
    **env_or_required_arg('VERBOSE', required=False))
parser.add_argument(
    '--objects-count',
    type=int,
    metavar='N',
    help="[DEBUGGING] Number of objects to copy. "
         "(env: S3_OBJECTS_COUNT)",
    **env_or_required_arg('S3_OBJECTS_COUNT', required=False))
