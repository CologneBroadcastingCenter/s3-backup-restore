import sys as sys

from .config import Config
from .log import logger


def put_metric(cw_metric_name, statistic_value, config=None,
               cw_dimension_name=None, cw_namespace=None):
    """Function that will put metrics to cloudwatch.

    This function puts metrics to cloudwatch.
    If statistic_value is 0 it will be converted to 0.1 it is not possible
    to 0 to cloudwatch.

    Args:
        cw_metric_name (str): Name of the cloudwatch metric.
        statistic_value (int, float): Value to send to cloudwatch as metric.
        config (s3backuprestore.config.Config): Configuration object
        for this class.
        cw_dimension_name (str, optional): Defaults to None. Cloudwatch
        dimension name.
        cw_namespace (str, optional): Defaults to None. Cloudwatch namespace.

    Raises:
        ValueError: If statistic_value is not convertible to float.
    """

    try:
        if config:
            session = config.boto3_session()
            cw_dimension_name = config.cw_dimension_name
            cw_namespace = config.cw_namespace
            region = config.region
        else:
            session = Config.boto3_session()
    except:
        logger.exception("")
        sys.exit(127)

    if not cw_dimension_name or not cw_metric_name:
        raise ValueError("You have to specify at least\
                         cw_dimension_name or config parameter")

    cw = session.resource('cloudwatch', region_name=region)
    try:
        float(statistic_value)
    except ValueError:
        logger.error("Statistic value not convertible to float.")

    try:
        if statistic_value == 0:
            statistic_value = 0.1

        cw.Metric(cw_namespace, cw_metric_name).put_data(
            MetricData=[
                {
                    'MetricName': cw_metric_name,
                    'Dimensions': [
                        {
                            'Name': cw_dimension_name,
                            'Value': cw_metric_name
                        }
                    ],
                    'StatisticValues': {
                        'SampleCount': statistic_value,
                        'Sum': statistic_value,
                        'Minimum': statistic_value,
                        'Maximum': statistic_value
                    },
                    'Unit': 'Count',
                    'StorageResolution': 1
                }
            ]
        )
    except:
        logger.exception("")
