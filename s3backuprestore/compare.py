import sys
import threading
import queue
import multiprocessing
import time
from random import randint
from datetime import datetime, timedelta, timezone

from .cw import put_metric
from .log import logger


class _Compare(threading.Thread):
    def __init__(self, config, compare_queue, copy_queue, max_wait=300,
                 cw_metric_name='CompareObjectsErrors'):
        """Class that compares objects and check if they are unequal.

        This class consumes compare_queue and compares objects between
        source bucket and destiantion bucket if they are unequal. If objects
        exists and are unequal they will be put into the copy queue and will
        be consumed elswehre.
        There are two kinds of parameters that will be checked.
        First the content length among the objects.
        Second the last modified time.
        If one of those parameters are unequal they will be added to the
        copy queue.

        Args:
            config (s3backuprestore.config.Config()): Configuration object
            for this class.
            compare_queue (Queue): A consumable queue like Queue.queue()
            copy_queue (Queue): A consumable queue like Queue.queue()
            max_wait (int, optional): Defaults to 300. If something went wrong
            and will be catched by an exception. We will wait for a particular
            time range. The range is between 1 and max_wait.
            cw_metric_name (str, optional): Defaults to 'CompareObjectsErrors'.
            Cloudwatch metric name where datapoint will be pushed to.
        """

        threading.Thread.__init__(self)
        self.config = config
        self.timeout = self.config.timeout
        self.src_bucket = self.config.src_bucket
        self.dst_bucket = self.config.dst_bucket
        self.last_modified = self.config.last_modified
        self.timedelta = datetime.now(timezone.utc) - timedelta(
            hours=self.last_modified)
        self.cw_namespace = self.config.cw_namespace
        self.cw_dimension_name = self.config.cw_dimension_name
        self.cw_metric_name = cw_metric_name
        self.compare_queue = compare_queue
        self.copy_queue = copy_queue
        self.max_wait = max_wait
        self.daemon = True
        self._session = config.boto3_session()

    def run(self):
        waiter = 1
        try:
            s3 = self._session.resource('s3')
        except:
            logger.exception("")
            put_metric(self.cw_metric_name, 1, self.config)
            sys.exit(127)

        while not self.compare_queue.empty():
            logger.debug("Compare queue size: {} keys"
                         .format(self.compare_queue.qsize()))
            try:
                key = self.compare_queue.get(timeout=self.timeout)
                logger.info("Got key {} from compare queue.".format(key))
            except queue.Empty:
                logger.warning("Compare queue seems empty. Checking again.")
                continue

            try:
                src_lm = s3.Object(self.src_bucket, key).last_modified
                logger.info("\n{}\nLastModified {}".format(key, src_lm))

                src_cl = s3.Object(self.src_bucket, key).content_length
                dst_cl = s3.Object(self.dst_bucket, key).content_length
                logger.info("\n{}\nSource ContentLength: \t{}\n"
                            "Destination ContentLength: \t{}"
                            .format(key, src_cl, dst_cl))

                waiter = max(round(waiter * 0.8), 1)
                logger.debug("Reduced waiting time to {}s.".format(waiter))
            except ConnectionRefusedError as exc:
                logger.error("Put {} back to queue.".format(key))
                logger.debug("", exc_info=True)
                put_metric(self.cw_metric_name, 1, self.config)
                self.compare_queue.put(key, timeout=self.timeout)
                logger.debug("Error occured sleeping for {}s.".format(waiter))
                time.sleep(waiter)

                # Set next waiting time
                waiter = randint(1, min(self.max_wait, waiter * 4))
                logger.debug("Next waiting time {}s.".format(waiter))
            except:
                logger.exception("Unhandeld exception occured.\n \
                                 Put {} back to compare queue.".format(key))
                put_metric(self.cw_metric_name, 1, self.config)
                self.compare_queue.put(key, timeout=self.timeout)

                logger.debug("Error occured sleeping for {}s.".format(waiter))
                time.sleep(waiter)
                # Increase waiting time
                waiter = randint(1, min(self.max_wait, waiter * 4))
                logger.debug("Next waiting time {}s.".format(waiter))
            else:
                if src_cl != dst_cl:
                    logger.info("Content length is unequal between"
                                "source and destination object.\n"
                                "Adding {} to copy queue.".format(key))
                    self.copy_queue.put(key, timeout=self.timeout)
                elif src_lm > self.timedelta:
                    logger.info("Object modified within last {}h.\n \
                                Adding {} to queue."
                                .format(self.last_modified, key))
                    self.copy_queue.put(key, timeout=self.timeout)
                logger.debug("Comparing for {} done.".format(key))


class MpCompare(multiprocessing.Process):
    def __init__(self, config, compare_queue, copy_queue, thread_count=5,
                 cw_metric_name='ObjectsToCompare'):
        """Class which will start _Compare

        [description]

        Args:
            config (s3backuprestore.config.Config): Configuration object
            for this class.
            compare_queue (Queue): A consumable queue like Queue.queue()
            copy_queue (Queue): A consumable queue like Queue.queue()
            thread_count (int, optional): Defaults to 5.
            cw_metric_name (str, optional): Defaults to 'ObjectsToCompare'.
            Cloudwatch metric name where datapoint will be pushed to.
        """

        multiprocessing.Process.__init__(self)
        self.config = config
        self.compare_queue = compare_queue
        self.copy_queue = copy_queue
        self.timeout = self.config.timeout
        self.thread_count = thread_count
        self.cw_metric_name = cw_metric_name

    def run(self):
        compare_queue_size = self.compare_queue.qsize()
        logger.debug("{} compare queue size {}"
                     .format(self.name, compare_queue_size))

        if compare_queue_size:
            thread_count = min(self.thread_count, compare_queue_size)

            # Start comparing S3 keys
            # Consume compare_queue until it is empty
            # Put all keys to copy_queue if they are different
            # between source bucket and destination bucket.
            th_lst = list()
            logger.info("{} starting {} threads."
                        .format(self.name, thread_count))

            # Starts number of threads specified in thread count.
            for t in range(thread_count):
                th_lst.append(_Compare(
                    self.config,
                    self.compare_queue,
                    self.copy_queue))
                logger.debug("{} {} generated."
                             .format(self.name, th_lst[t].name))
                th_lst[t].start()
                logger.debug("{} {} started."
                             .format(self.name, th_lst[t].name))
            logger.debug("{} started {} threads."
                         .format(self.name, len(th_lst)))

            try:
                logger.debug("{} checking compare queue size if empty."
                             .format(self.name))
                while not self.compare_queue.empty():
                    logger.info("{} compare queue not empty {} keys."
                                "Waiting for 60s."
                                .format(self.name, self.compare_queue.qsize()))
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
                    logger.debug("{} try joining compare {}."
                                 .format(self.name, th_lst[t].name))
                    th_lst[t].join(timeout=self.timeout)
                    logger.debug("{} {} joined."
                                 .format(self.name, th_lst[t].name))
            logger.info("{} all compare threads finished.".format(self.name))
        else:
            logger.warning("No objects to compare for {}!".format(self.name))
