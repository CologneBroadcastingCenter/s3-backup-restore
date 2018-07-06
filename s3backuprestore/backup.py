import multiprocessing
import queue
import sys
import threading
import time
from random import randint
from botocore.exceptions import ClientError, EndpointConnectionError

from .cw import put_metric
from .log import logger


class _Backup(threading.Thread):
    def __init__(self, config, copy_queue, max_wait=300,
                 cw_metric_name='BackupObjectsErrors'):
        """Class which will copy objects from source bucket to
        destination bucket using boto3s copy method.

        This class is inherited from threading.Thread it copies s3 objects
        using boto3's copy() method. You should not use this method if you are
        not really sure why.

        You have to provide a configuration object provided by
        s3backuprestore.config.Config() and a consumable queue filled
        with S3 keys.

        Args:
            config (s3backuprestore.config.Config()): Configuration object
            for this class.
            copy_queue (Queue): A consumable queue like Queue.queue()
            max_wait (int, optional): Defaults to 300. Waiting time if there
            are any errors, holds the thread and starts it after a range
            between 1 and max_wait.
            cw_metric_name (str, optional): Defaults to 'BackupObjectsErrors'.
            Cloudwatch metric name where datapoint will be pushed to.
        """
        threading.Thread.__init__(self)
        self.config = config
        self.timeout = self.config.timeout
        self.src_bucket = self.config.src_bucket
        self.dst_bucket = self.config.dst_bucket
        self.cw_namespace = self.config.cw_namespace
        self.cw_dimension_name = self.config.cw_dimension_name
        self.extra_args = self.config.extra_args
        self.cw_metric_name = cw_metric_name
        self.copy_queue = copy_queue
        self.max_wait = max_wait
        self.daemon = True
        self._session = self.config.boto3_session()
        self._transfer_mgr = self.config.s3_transfer_manager()

    def run(self):
        waiter = 1
        try:
            s3 = self._session.resource('s3')
        except:
            logger.exception("")
            put_metric(self.cw_metric_name, 1, self.config)
            sys.exit(127)

        while not self.copy_queue.empty():
            logger.debug("Copy queue size: {} keys"
                         .format(self.copy_queue.qsize()))
            try:
                key = self.copy_queue.get(timeout=self.timeout)
                logger.info("Got key {} from copy queue.".format(key))
            except queue.Empty as exc:
                logger.warning("Copy queue seems empty. Checking again.")
                continue

            # Preparing copy task
            dst_obj = s3.Object(self.dst_bucket, key)
            cp_src = {'Bucket': self.src_bucket, 'Key': key}
            try:
                logger.info("{} copying {}".format(self.name, key))
                dst_obj.copy(
                    cp_src,
                    ExtraArgs=self.extra_args,
                    Config=self._transfer_mgr)
            except ClientError as exc:
                try:
                    error_code = exc.response['Error']['Code']
                    if 'SlowDown' in error_code:
                        logger.warning("SlowDown occurs. Waiting for {:.0f}s"
                                       .format(waiter))
                        logger.debug("{}\n Key {}".format(exc.response, key))
                        put_metric('SlowDown', 1, self.config)
                    elif 'InternalError' in error_code:
                        logger.warning("InternalError occurs. Waiting for "
                                       "{:.0f}s".format(waiter))
                        put_metric(self.cw_metric_name, 1, self.config)
                        logger.debug("{}\n Key {}".format(exc.response, key))
                    else:
                        logger.error("{}\n Key {}".format(exc.response, key))
                        put_metric(self.cw_metric_name, 1, self.config)
                except KeyError as exc:
                    if "reached max retries" in str(exc.__context__):
                        logger.warning("Max retries reached.")
                        logger.debug(exc.__context__)
                    else:
                        logger.exception("No Errcode in exception response.")
                        logger.debug(exc.__context__)
                    self.copy_queue.put(key, timeout=self.timeout)
                    put_metric(self.cw_metric_name, 1, self.config)
                    logger.debug("Error occured sleeping for {}s."
                                 .format(waiter))
                    time.sleep(waiter)
                    # Increase maximum of waiting time
                    waiter = randint(1, min(self.max_wait, waiter * 4))
                    logger.debug("Next waiting time {}s.".format(waiter))
                else:
                    logger.error("Put {} back to queue.".format(key))
                    self.copy_queue.put(key, timeout=self.timeout)
                    logger.debug("Error occured sleeping for {}s."
                                 .format(waiter))
                    time.sleep(waiter)
                    # Increase maximum of waiting time
                    waiter = randint(1, min(self.max_wait, waiter * 4))
                    logger.debug("Next waiting time {}s.".format(waiter))
            except ConnectionRefusedError as exc:
                logger.exception("Waiting for {:.0f}s.\n"
                                 "Put {} back to queue.\n"
                                 "Maybe to many connections?"
                                 .format(waiter, key))
                put_metric(self.cw_metric_name, 1, self.config)
                self.copy_queue.put(key, timeout=self.timeout)
                logger.debug("Error occured sleeping for {}s.".format(waiter))
                time.sleep(waiter)
                waiter = randint(1, min(self.max_wait, waiter * 4))
                logger.debug("Next waiting time {}s.".format(waiter))
            except EndpointConnectionError as exc:
                logger.warning("EndpointConnectionError.\n"
                               "Waiting for {:.0f}s.\n"
                               "Put {} back to queue.\n"
                               .format(waiter, key))
                put_metric(self.cw_metric_name, 1, self.config)
                self.copy_queue.put(key, timeout=self.timeout)
                logger.debug("Error occured sleeping for {}s.".format(waiter))
                time.sleep(waiter)
                waiter = randint(1, min(self.max_wait, waiter * 4))
                logger.debug("Next waiting time {}s.".format(waiter))
            except:
                logger.exception("Unhandeld exception occured.\n \
                                 Put {} back to queue.".format(key))
                put_metric(self.cw_metric_name, 1, self.config)
                self.copy_queue.put(key, timeout=self.timeout)
                logger.debug("Error occured sleeping for {}s.".format(waiter))
                time.sleep(waiter)
                waiter = randint(1, min(self.max_wait, waiter * 4))
                logger.debug("Next waiting time {}s.".format(waiter))
            else:
                logger.info("{} copied {}".format(self.name, key))
                # Reduce waiting time
                waiter = max(round(waiter * 0.8), 1)
                logger.debug("Reduced waiting time to {}s.".format(waiter))


class MpBackup(multiprocessing.Process):
    def __init__(self, config, copy_queue, thread_count=10,
                 cw_metric_name='ObjectsToCopy'):
        """Class which will start _Backup() threads.

        This class will start processes with _Backup() threads so that they can
        consume the copy queue until its empty. Each object will spawn a new
        child process.

         Args:
            config (s3backuprestore.config.Config): Configuration object
            for this class.
            copy_queue (Queue): A consumable queue, like Queue.queue().
            thread_count (int, optional): Defaults to 10. Number of threads
            which will be spawned in each process.
            cw_metric_name (str, optional): Defaults to 'ObjectsToCopy'.
            Cloudwatch metric name where datapoint will be pushed to.
        """

        multiprocessing.Process.__init__(self)
        self.config = config
        self.copy_queue = copy_queue
        self.timeout = self.config.timeout
        self.thread_count = thread_count
        self.cw_metric_name = cw_metric_name

    def run(self):
        copy_queue_size = self.copy_queue.qsize()
        logger.debug("{} copy queue size {}"
                     .format(self.name, copy_queue_size))
        if copy_queue_size:
            thread_count = min(self.thread_count, copy_queue_size)

            # Start copying S3 objects from
            # source bucket to destiantion bucket
            # Consume copy_queue until it is empty
            th_lst = list()
            logger.info("{} starting {} threads."
                        .format(self.name, thread_count))
            for t in range(thread_count):
                th_lst.append(_Backup(
                    self.config,
                    self.copy_queue))
                logger.debug("{} {} generated."
                             .format(self.name, th_lst[t].name))
                th_lst[t].start()
                logger.debug("{} {} started."
                             .format(self.name, th_lst[t].name))

            try:
                logger.debug("Checking copy queue size if empty.")
                while not self.copy_queue.empty():
                    logger.info("Copy queue not empty {} keys. \
                                Waiting for 60s."
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
                    logger.debug("{} copy {} joined."
                                 .format(self.name, th_lst[t].name))
            logger.info("{} all copy threads finished.".format(self.name))
        else:
            logger.warning("No objects to copy for {}!".format(self.name))
