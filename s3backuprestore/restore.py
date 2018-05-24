import multiprocessing
import queue
import sys
import threading
import time

from .cw import put_metric
from .log import logger


class Restore(threading.Thread):
    """This class provides an easy interface of restoring S3 objects.
       It uses the copy method from boto3 to only copy all S3 objects
       server side, to avoid downloading and uploading it and speed
       up transfere time.

        Inheritance:
            threading.Thread
    """

    def __init__(self, config, restore_queue,
                 cw_metric_name='RestoreObjectsErrors'):
        threading.Thread.__init__(self)
        self.config = config
        self.timeout = self.config.timeout
        self.src_bucket = self.config.src_bucket
        self.dst_bucket = self.config.dst_bucket
        self.cw_namespace = self.config.cw_namespace
        self.cw_dimension_name = self.config.cw_dimension_name
        self.cw_metric_name = cw_metric_name
        self.restore_queue = restore_queue
        self.daemon = True
        self._session = config.boto3_session()

    def run(self):
        """Run method of threading.Thread class.

        Creates an S3 resource from boto3.
        Receives keys from global scoped queue and starts copying them
        to provided destination bucket.
        The copy job starts by default with 10 threads and does detect if
        multipart upload and retry is needed.
        """

        waiter = 1
        try:
            s3 = self._session.resource('s3')
        except:
            logger.exception("")
            put_metric(self.cw_metric_name, 1, self.config)
            sys.exit(127)

        while not self.restore_queue.empty():
            try:
                key = self.restore_queue.get(timeout=self.timeout)
            except queue.Empty as exc:
                continue

            try:
                storage_class = s3.Object(self.src_bucket, key).storage_class
            except s3.meta.client.exceptions.ClientError as exc:
                try:
                    error_code = exc.response['Error']['Code']
                    if 'SlowDown' in error_code:
                        logger.warning("SlowDown occurs. Waiting for {}s"
                                       .format(waiter))
                        put_metric('SlowDown', 1, self.config)
                    else:
                        logger.error(exc.response)
                        put_metric(self.cw_metric_name, 1, self.config)
                except KeyError:
                    logger.exception("No Error Code in exception response")
                    self.restore_queue.put(key)
                    put_metric(self.cw_metric_name, 1, self.config)
                    time.sleep(waiter)
                    # Increase waiting time
                    waiter *= 2
                else:
                    self.restore_queue.put(key)
                    time.sleep(waiter)
                    # Increase waiting time
                    waiter *= 2
            else:
                # Reduce waiting time
                if waiter > 1:
                    waiter /= 2

            if storage_class and 'GLACIER' in storage_class:
                logger.warning("{} still in Glacier. Waiting {}s."
                               .format(key, waiter))

                self.restore_queue.put(key)
                # Increasing waiting time
                time.sleep(waiter)
                waiter *= 2
            else:
                # Preparing copy task
                dst_obj = s3.Object(self.dst_bucket, key)
                logger.info("{} copying key {}".format(self.name, key))
                try:
                    cp_src = {'Bucket': self.src_bucket, 'Key': key}
                    dst_obj.copy(cp_src)
                except s3.meta.client.exceptions.ClientError as exc:
                    try:
                        error_code = exc.response['Error']['Code']
                        if 'SlowDown' in error_code:
                            logger.warning("SlowDown occurs.")
                            put_metric('SlowDown', 1, self.config)
                        else:
                            logger.error(exc.response)
                            put_metric(self.cw_metric_name, 1, self.config)
                    except KeyError:
                        logger.exception("No Error Code in exception response")
                        put_metric(self.cw_metric_name, 1, self.config)
                        self.restore_queue.put(key)
                        time.sleep(waiter)
                        # Increase waiting time
                        waiter *= 2
                    else:
                        self.restore_queue.put(key)
                        time.sleep(waiter)
                        # Increase waiting time
                        waiter *= 2
                        logger.debug("{} is sleeping for {}s."
                                     .format(self.name, waiter))
                except ConnectionRefusedError as exc:
                    logger.exception("Waiting for {}s.\n"
                                     "Put {} back to queue.\n"
                                     "Maybe to many connections?"
                                     .format(waiter, key))
                    put_metric(self.cw_metric_name, 1, self.config)
                    self.restore_queue.put(key)
                    time.sleep(waiter)
                    waiter *= 2
                except:
                    logger.exception("Unhandeld exception occured.\n"
                                     "Waiting for {}s. Put {} back to queue."
                                     .format(key))
                    put_metric(self.cw_metric_name, 1, self.config)
                    self.restore_queue.put(key)
                    time.sleep(waiter)
                    waiter *= 2
                else:
                    self.restore_queue.task_done()
                    # Decreasing waiting time
                    if waiter > 1:
                        waiter /= 2


class MpRestore(multiprocessing.Process):
    def __init__(self, config, restore_queue, thread_count=10,
                 cw_metric_name='ObjectsToRestore'):
        multiprocessing.Process.__init__(self)
        self.config = config
        self.restore_queue = restore_queue
        self.timeout = self.config.timeout
        self.thread_count = thread_count
        self.cw_metric_name = cw_metric_name

    def run(self):
        restore_queue_size = self.restore_queue.qsize()

        if restore_queue_size:
            thread_count = min(self.thread_count, restore_queue_size)

            # Start copying S3 objects to destiantion bucket
            # Consume restore_queue until it is empty
            th_lst = list()
            logger.info("{} starting {} threads."
                        .format(self.name, thread_count))
            for t in range(thread_count):
                th_lst.append(Restore(
                    self.config,
                    self.restore_queue))
                logger.debug("{} {} generated."
                             .format(self.name, th_lst[t].name))
                th_lst[t].start()
                logger.debug("{} {} started."
                             .format(self.name, th_lst[t].name))

            try:
                while not self.restore_queue.empty():
                        time.sleep(15)
            except KeyboardInterrupt:
                logger.info("Exiting...")
                sys.exit(127)
            else:
                self.restore_queue.join()

            logger.info("{} joining all threads.".format(self.name))
            for t in range(thread_count):
                logger.debug("{} waiting for {} to be finished."
                             .format(self.name, th_lst[t].name))
                try:
                    while th_lst[t].is_alive():
                        time.sleep(15)
                except KeyboardInterrupt:
                    logger.warning("Exiting...")
                    sys.exit(127)
                else:
                    th_lst[t].join(timeout=self.timeout)
                    logger.debug("{} {} finished."
                                 .format(self.name, th_lst[t].name))
            logger.info("{} all threads finished.".format(self.name))
        else:
            logger.warning("No objects to restore for {}!".format(self.name))
