import argparse

parser = argparse.ArgumentParser(add_help=False)
parser.add_argument(
    '--source-bucket',
    '-s',
    help="Source Bucket to sync diff from.",
    required=True
)
parser.add_argument(
    '--destination-bucket',
    '-d',
    help="Destination Bucket to sync to.",
    required=True
)
parser.add_argument(
    '--prefix',
    default='',
    help="Prefix to list objects from and to sync to."
)
parser.add_argument(
    '-a',
    '--all',
    help="Copies all objects. "
         "It doesn't compare, tag nor check objects in destination."
         "Use this option only if you want a full copy of your backup!",
    action='store_true',
    required=False
)
parser.add_argument(
    '--thread-count',
    metavar='N',
    type=int,
    help="Starting count for threads.",
    required=False
)
parser.add_argument(
    '--timeout',
    default=30,
    type=int,
    metavar='s',
    required=False,
    help="Sets the timeout to finish each task before skipping."
)
parser.add_argument(
    '--profile',
    '-p',
    help="AWS profile name configure in aws config files."
)
parser.add_argument(
    '--region',
    default='eu-central-1',
    help="AWS Region"
)
parser.add_argument(
    '--verbose', '-v',
    action='count'
)
parser.add_argument(
    '--objects-count',
    help='[DEBUGGING] Number of objects to copy.',
    required=False,
    type=int,
    metavar='N'
)
