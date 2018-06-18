import multiprocessing
import queue
import sys
import threading
import time
from random import randint
from botocore.exceptions import ClientError, EndpointConnectionError

from .cw import put_metric
from .log import logger


class _Restore(threading.Thread):
    def __init__(self, config, restore_queue, max_wait=300,
                 cw_metric_name='RestoreObjectsErrors'):
        """This class provides an easy interface of restoring S3 objects.
        It uses the copy method from boto3 to only copy all S3 objects
        server side, to avoid downloading and uploading it and speed
        up transfere time.

        Inheritance:
            threading.Thread
        """
        threading.Thread.__init__(self)
        self.config = config
        self.timeout = self.config.timeout
        self.src_bucket = self.config.src_bucket
        self.dst_bucket = self.config.dst_bucket
        self.cw_namespace = self.config.cw_namespace
        self.cw_dimension_name = self.config.cw_dimension_name
        self.cw_metric_name = cw_metric_name
        self.restore_queue = restore_queue
        self.max_wait = max_wait
        self.daemon = True
        self._session = config.boto3_session()
        self._transfer_mgr = config.s3_transfer_manager()

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
            logger.debug("Restore queue size: {} keys"
                         .format(self.copy_queue.qsize()))
            try:
                key = self.restore_queue.get(timeout=self.timeout)
                logger.info("Got key {} from restore queue.".format(key))
            except queue.Empty as exc:
                logger.warning("Restore queue seems empty. Checking again.")
                continue

            # Getting storage class of object.
            try:
                storage_class = s3.Object(self.src_bucket, key).storage_class
            except ClientError as exc:
                try:
                    error_code = exc.response['Error']['Code']
                    if 'SlowDown' in error_code:
                        logger.warning("SlowDown occurs. Waiting for {}s"
                                       .format(waiter))
                        logger.debug("{}\n Key {}".format(exc.response, key))
                        put_metric('SlowDown', 1, self.config)
                    elif 'InternalError' in error_code:
                        logger.warning("InternalError occurs. Waiting for "
                                       "{}s".format(waiter))
                        logger.debug("{}\n Key {}".format(exc.response, key))
                        put_metric(self.cw_metric_name, 1, self.config)
                    else:
                        logger.error("{}\n Key {}".format(exc.response, key))
                        put_metric(self.cw_metric_name, 1, self.config)
                except KeyError as exc:
                    if "reached max retries" in str(exc.__context__):
                        logger.warning("Max retries reached.")
                        logger.debug(exc.__context__)
                    else:
                        logger.exception("No Error Code in exception response")
                        logger.debug(exc.__context__)
                    self.restore_queue.put(key, timeout=self.timeout)
                    put_metric(self.cw_metric_name, 1, self.config)
                    logger.debug("Error occured sleeping for {}s."
                                 .format(waiter))
                    time.sleep(waiter)
                    # Increase waiting time
                    waiter = randint(1, min(self.max_wait, waiter * 4))
                    logger.debug("Next waiting time {}s.".format(waiter))
                else:
                    logger.error("Put {} back to queue.".format(key))
                    self.restore_queue.put(key, timeout=self.timeout)
                    time.sleep(waiter)
                    # Increase waiting time
                    waiter = randint(1, min(self.max_wait, waiter * 4))
                    logger.debug("Next waiting time {}s.".format(waiter))
            except EndpointConnectionError as exc:
                logger.warning("EndpointConnectionError.\n"
                               "Waiting for {}s.\n"
                               "Put {} back to queue.\n"
                               .format(waiter, key))
                put_metric(self.cw_metric_name, 1, self.config)
                self.restore_queue.put(key, timeout=self.timeout)
                logger.debug("Error occured sleeping for {}s.".format(waiter))
                time.sleep(waiter)
                # Increase waiting time
                waiter = randint(1, min(self.max_wait, waiter * 4))
                logger.debug("Next waiting time {}s.".format(waiter))
            except:
                logger.exception("Unhandeld exception occured.\n \
                                 Put {} back to queue.".format(key))
                put_metric(self.cw_metric_name, 1, self.config)
                self.restore_queue.put(key, timeout=self.timeout)
                logger.debug("Error occured sleeping for {}s.".format(waiter))
                time.sleep(waiter)
                # Increase waiting time
                waiter = randint(1, min(self.max_wait, waiter * 4))
                logger.debug("Next waiting time {}s.".format(waiter))
            else:
                logger.info("Object {} has storage class {}"
                            .format(key, storage_class))
                # Reduce waiting time
                waiter = max(round(waiter * 0.8), 1)
                logger.debug("Reduced waiting time to {}s.".format(waiter))

            # Checking objects storage class.
            # If objects storage class equals GLACIER put it into
            # _glacier_queue to process it later.
            if storage_class and 'GLACIER' in storage_class:
                logger.warning("{} in Glacier. Waiting {}s."
                               .format(key, waiter))

                self.restore_queue.put(key, timeout=self.timeout)
                # Increasing waiting time
                time.sleep(waiter)
                waiter = randint(1, min(self.max_wait, waiter * 4))
                logger.debug("Next waiting time {}s.".format(waiter))
            else:
                # Preparing copy task
                dst_obj = s3.Object(self.dst_bucket, key)
                cp_src = {'Bucket': self.src_bucket, 'Key': key}
                try:
                    logger.info("{} copying {}".format(self.name, key))
                    dst_obj.copy(cp_src, Config=self._transfer_mgr)
                except ClientError as exc:
                    try:
                        error_code = exc.response['Error']['Code']
                        if 'SlowDown' in error_code:
                            logger.warning("SlowDown occurs. "
                                           "Waiting for {:.0f}s"
                                           .format(waiter))
                            logger.debug("{}\n Key {}"
                                         .format(exc.response, key))
                            put_metric('SlowDown', 1, self.config)
                        elif 'InternalError' in error_code:
                            logger.warning("InternalError occurs. Waiting for "
                                           "{:.0f}s".format(waiter))
                            put_metric(self.cw_metric_name, 1, self.config)
                            logger.debug("{}\n Key {}"
                                         .format(exc.response, key))
                        else:
                            logger.error("{}\n Key {}"
                                         .format(exc.response, key))
                            put_metric(self.cw_metric_name, 1, self.config)
                    except KeyError:
                        if "reached max retries" in str(exc.__context__):
                            logger.warning("Max retries reached.")
                            logger.debug(exc.__context__)
                        else:
                            logger.exception("No Errcode in "
                                             "exception response.")
                            logger.debug(exc.__context__)
                        self.restore_queue.put(key)
                        put_metric(self.cw_metric_name, 1, self.config)
                        logger.debug("Error occured sleeping for {}s."
                                     .format(waiter))
                        time.sleep(waiter)
                        # Increase waiting time
                        waiter = randint(1, min(self.max_wait, waiter * 4))
                        logger.debug("Next waiting time {}s.".format(waiter))
                    else:
                        logger.error("Put {} back to queue.".format(key))
                        self.restore_queue.put(key, timeout=self.timeout)
                        logger.debug("Error occured sleeping for {}s."
                                     .format(waiter))
                        time.sleep(waiter)
                        # Increase waiting time
                        waiter = randint(1, min(self.max_wait, waiter * 4))
                        logger.debug("Next waiting time {}s.".format(waiter))
                except ConnectionRefusedError as exc:
                    logger.exception("Waiting for {:.0f}s.\n"
                                     "Put {} back to queue.\n"
                                     "Maybe to many connections?"
                                     .format(waiter, key))
                    put_metric(self.cw_metric_name, 1, self.config)
                    self.restore_queue.put(key, timeout=self.timeout)
                    logger.debug("Error occured sleeping for {}s."
                                 .format(waiter))
                    time.sleep(waiter)
                    waiter = randint(1, min(self.max_wait, waiter * 4))
                    logger.debug("Next waiting time {}s.".format(waiter))
                except EndpointConnectionError as exc:
                    logger.warning("EndpointConnectionError.\n"
                                   "Waiting for {:.0f}s.\n"
                                   "Put {} back to queue.\n"
                                   .format(waiter, key))
                    put_metric(self.cw_metric_name, 1, self.config)
                    self.restore_queue.put(key, timeout=self.timeout)
                    logger.debug("Error occured sleeping for {}s."
                                 .format(waiter))
                    time.sleep(waiter)
                    waiter = randint(1, min(self.max_wait, waiter * 4))
                    logger.debug("Next waiting time {}s.".format(waiter))
                except:
                    logger.exception("Unhandeld exception occured.\n "
                                     "Put {} back to queue.".format(key))
                    put_metric(self.cw_metric_name, 1, self.config)
                    self.restore_queue.put(key, timeout=self.timeout)
                    logger.debug("Error occured sleeping for {}s."
                                 .format(waiter))
                    waiter = randint(1, min(self.max_wait, waiter * 4))
                    logger.debug("Next waiting time {}s.".format(waiter))
                else:
                    logger.info("{} copied {}".format(self.name, key))
                    # Reduce waiting time
                    waiter = max(round(waiter * 0.8), 1)
                    logger.debug("Reduced waiting time to {}s.".format(waiter))


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
        logger.debug("{} restore queue size {}"
                     .format(self.name, restore_queue_size))
        if restore_queue_size:
            thread_count = min(self.thread_count, restore_queue_size)

            # Start copying S3 objects to destiantion bucket
            # Consume restore_queue until it is empty
            th_lst = list()
            logger.info("{} starting {} threads."
                        .format(self.name, thread_count))
            for t in range(thread_count):
                th_lst.append(_Restore(
                    self.config,
                    self.restore_queue))
                logger.debug("{} {} generated."
                             .format(self.name, th_lst[t].name))
                th_lst[t].start()
                logger.debug("{} {} started."
                             .format(self.name, th_lst[t].name))

            try:
                logger.debug("Checking restore queue size if empty.")
                while not self.restore_queue.empty():
                    logger.info("Restore queue not empty {} keys. "
                                "Waiting for 60s."
                                .format(self.copy_queue.qsize()))
                    time.sleep(60)
            except KeyboardInterrupt:
                logger.info("Exiting...")
                sys.exit(127)

            logger.info("{} joining all threads.".format(self.name))
            for t in range(thread_count):
                logger.debug("{} waiting for {} to be finished."
                             .format(self.name, th_lst[t].name))
                try:
                    while th_lst[t].is_alive():
                        logger.debug("In {} {} is still alive. \
                                     Waiting for 60s."
                                     .format(self.name, th_lst[t].name))
                        time.sleep(60)
                except KeyboardInterrupt:
                    logger.warning("Exiting...")
                    sys.exit(127)
                else:
                    logger.debug("Try joining copy {}.".format(th_lst[t].name))
                    th_lst[t].join(timeout=self.timeout)
                    logger.debug("{} {} joined."
                                 .format(self.name, th_lst[t].name))
            logger.info("{} all restore threads finished.".format(self.name))
        else:
            logger.warning("No objects to restore for {}!".format(self.name))
