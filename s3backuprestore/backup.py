import multiprocessing
import queue
import sys
import threading
import time
from botocore.exceptions import ClientError, EndpointConnectionError

from .cw import put_metric
from .log import logger


class Backup(threading.Thread):
    def __init__(self, config, copy_queue,
                 cw_metric_name='BackupObjectsErrors'):
        threading.Thread.__init__(self)
        self.config = config
        self.timeout = self.config.timeout
        self.src_bucket = self.config.src_bucket
        self.dst_bucket = self.config.dst_bucket
        self.cw_namespace = self.config.cw_namespace
        self.cw_dimension_name = self.config.cw_dimension_name
        self.cw_metric_name = cw_metric_name
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

        while not self.copy_queue.empty():
            try:
                key = self.copy_queue.get(timeout=self.timeout)
            except queue.Empty as exc:
                continue

            # Preparing copy task
            dst_obj = s3.Object(self.dst_bucket, key)
            logger.info("{} copying {}".format(self.name, key))
            try:
                cp_src = {'Bucket': self.src_bucket, 'Key': key}
                dst_obj.copy(cp_src)
            except ClientError as exc:
                try:
                    error_code = exc.response['Error']['Code']
                    if 'SlowDown' in error_code:
                        logger.warning("SlowDown occurs. Waiting for {:.0f}s"
                                       .format(waiter))
                        put_metric('SlowDown', 1, self.config)
                    else:
                        logger.error("{}\n Key {}".format(exc.response, key))
                        put_metric(self.cw_metric_name, 1, self.config)
                except KeyError as exc:
                    if "reached max retries" in str(exc.__context__):
                        logger.warning("Max retries reached.")
                    else:
                        logger.exception("No Error Code in exception response")
                    self.copy_queue.put(key)
                    put_metric(self.cw_metric_name, 1, self.config)
                    time.sleep(waiter)
                    # Increase waiting time
                    waiter = min(90, waiter * 4)
                else:
                    self.copy_queue.put(key)
                    time.sleep(waiter)
                    # Increase waiting time
                    waiter = min(90, waiter * 4)
            except ConnectionRefusedError as exc:
                logger.exception("Waiting for {:.0f}s.\n"
                                 "Put {} back to queue.\n"
                                 "Maybe to many connections?"
                                 .format(waiter, key))
                put_metric(self.cw_metric_name, 1, self.config)
                self.copy_queue.put(key)
                time.sleep(waiter)
                waiter = min(90, waiter * 4)
            except EndpointConnectionError as exc:
                logger.warning("EndpointConnectionError.\n"
                               "Waiting for {:.0f}s.\n"
                               "Put {} back to queue.\n"
                               .format(waiter, key))
                put_metric(self.cw_metric_name, 1, self.config)
                self.copy_queue.put(key)
                time.sleep(waiter)
                waiter = min(90, waiter * 4)
            except:
                logger.exception("Unhandeld exception occured.\n"
                                 "Put {} back to queue.".format(key))
                put_metric(self.cw_metric_name, 1, self.config)
                self.copy_queue.put(key)
                time.sleep(waiter)
                # Increase waiting time
                waiter = min(90, waiter * 4)
            else:
                self.copy_queue.task_done()
                # Reduce waiting time
                waiter = max(1, waiter * 0.80)


class MpBackup(multiprocessing.Process):
    def __init__(self, config, copy_queue, thread_count=10,
                 cw_metric_name='ObjectsToCopy'):
        multiprocessing.Process.__init__(self)
        self.config = config
        self.copy_queue = copy_queue
        self.timeout = self.config.timeout
        self.thread_count = thread_count
        self.cw_metric_name = cw_metric_name

    def run(self):
        copy_queue_size = self.copy_queue.qsize()

        if copy_queue_size:
            thread_count = min(self.thread_count, copy_queue_size)

            # Start copying S3 objects to destiantion bucket
            # Consume copy_queue until it is empty
            th_lst = list()
            logger.info("{} starting {} threads."
                        .format(self.name, thread_count))
            for t in range(thread_count):
                th_lst.append(Backup(
                    self.config,
                    self.copy_queue))
                logger.debug("{} {} generated."
                             .format(self.name, th_lst[t].name))
                th_lst[t].start()
                logger.debug("{} {} started."
                             .format(self.name, th_lst[t].name))

            try:
                while not self.copy_queue.empty():
                        time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Exiting...")
                sys.exit(127)
            else:
                self.copy_queue.join()

            logger.info("{} joining all threads.".format(self.name))
            for t in range(thread_count):
                logger.debug("{} waiting for {} to be finished."
                             .format(self.name, th_lst[t].name))
                try:
                    while th_lst[t].is_alive():
                        time.sleep(1)
                except KeyboardInterrupt:
                    logger.warning("Exiting...")
                    sys.exit(127)
                else:
                    th_lst[t].join(timeout=self.timeout)
                    logger.debug("{} {} finished."
                                 .format(self.name, th_lst[t].name))
            logger.info("{} all threads finished.".format(self.name))
        else:
            logger.warning("No objects to copy for {}!".format(self.name))
