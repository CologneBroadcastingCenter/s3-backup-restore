import sys
import threading
import queue
import multiprocessing
import time
from datetime import datetime, timedelta, timezone

from .cw import put_metric
from .log import logger


class Compare(threading.Thread):
    def __init__(self, config, compare_queue, copy_queue,
                 cw_metric_name='CompareObjectsErrors'):
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
            try:
                key = self.compare_queue.get(timeout=self.timeout)
            except queue.Empty:
                continue

            try:
                src_lm = s3.Object(self.src_bucket, key).last_modified
                logger.debug("\n{}\nLastModified {}".format(key, src_lm))

                src_cl = s3.Object(self.src_bucket, key).content_length
                dst_cl = s3.Object(self.dst_bucket, key).content_length
                logger.debug("\n{}\nSource ContentLength: \t{}\n"
                             "Destination ContentLength: \t{}"
                             .format(key, src_cl, dst_cl))

                # Reduce waiting time
                if waiter > 1:
                    waiter /= 2

            except ConnectionRefusedError as exc:
                logger.exception("Put {} back to queue.".format(key))
                put_metric(self.cw_metric_name, 1, self.config)
                self.compare_queue.put(key)
                time.sleep(waiter)
                # Increase waiting time
                waiter *= 2
            except:
                logger.exception("Unhandeld exception occured.\n"
                                 "Put {} back to queue.".format(key))
                put_metric(self.cw_metric_name, 1, self.config)
                time.sleep(waiter)
                # Increase waiting time
                waiter *= 2
            else:
                if src_cl != dst_cl:
                    logger.info("Content length is unequal between"
                                "source and destination object.\n"
                                "Adding {} to copy queue.".format(key))
                    self.copy_queue.put(key)
                elif src_lm > self.timedelta:
                    logger.info("Object modified within last {}h.\n " +
                                "Adding {} to queue."
                                .format(self.last_modified, key))
                    self.copy_queue.put(key)
                self.compare_queue.task_done()


class MpCompare(multiprocessing.Process):
    def __init__(self, config, compare_queue, copy_queue, thread_count=5,
                 cw_metric_name='ObjectsToCompare'):
        multiprocessing.Process.__init__(self)
        self.config = config
        self.compare_queue = compare_queue
        self.copy_queue = copy_queue
        self.timeout = self.config.timeout
        self.thread_count = thread_count
        self.cw_metric_name = cw_metric_name

    def run(self):
        compare_queue_size = self.compare_queue.qsize()

        if compare_queue_size:
            thread_count = min(self.thread_count, compare_queue_size)

            # Start copying S3 objects to destiantion bucket
            # Consume copy_queue until it is empty
            th_lst = list()
            logger.info("{} starting {} threads."
                        .format(self.name, thread_count))
            for t in range(thread_count):
                th_lst.append(Compare(
                    self.config,
                    self.compare_queue,
                    self.copy_queue))
                logger.debug("{} {} generated."
                             .format(self.name, th_lst[t].name))
                th_lst[t].start()
                logger.debug("{} {} started."
                             .format(self.name, th_lst[t].name))

            try:
                while not self.compare_queue.empty():
                        time.sleep(15)
            except KeyboardInterrupt:
                logger.info("Exiting...")
                sys.exit(127)
            else:
                self.compare_queue.join()

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
            logger.warning("No objects to compare for {}!"
                           .format(self.name))
