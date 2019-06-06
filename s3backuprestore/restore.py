import multiprocessing
import queue
import sys
import threading
import time
from random import randint
from botocore.exceptions import ClientError, EndpointConnectionError
import boto3

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
        self.dst_bucket_role = self.config.dst_bucket_role
        self.cw_namespace = self.config.cw_namespace
        self.cw_dimension_name = self.config.cw_dimension_name
        self.cw_metric_name = cw_metric_name
        self.restore_queue = restore_queue
        self.max_wait = max_wait
        self.waiter = 1
        # Sets the thred in daemon mode. See:
        # https://docs.python.org/3/library/threading.html#threading.Thread.daemon
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

        try:

            s3 = self._session.resource('s3')

        except Exception as exc:
            logger.exception("")
            put_metric(self.cw_metric_name, 1, self.config)
            sys.exit(127)

        while not self.restore_queue.empty():
            logger.debug("Restore queue size: "
                         f"{self.restore_queue.qsize()} keys")
            try:
                key = self.restore_queue.get(timeout=self.timeout)
                logger.info(f"Got key {key} from restore queue.")
            except queue.Empty as exc:
                logger.warning("Restore queue seems empty. Checking again.")
                continue

            ret = self._get_storage_class(s3, self.src_bucket, key)
            storage_class = ret.get('StorageClass', None)
            ongoing_req = ret.get('OngoingRequest', None)

            # Checking objects storage class.
            # If objects storage class equals GLACIER put it into
            # _glacier_queue to process it later.
            logger.info("Checking if object is in GLACIER and "
                        f"ongoing-request is false for {key}.")
            if (storage_class and 'GLACIER' in storage_class and
                    (not ongoing_req or 'ongoing-request="true"' in ongoing_req)):

                logger.info(f"Request is ongoing for {key}. "
                            f"Waiting {self.waiter}s.")

                self.restore_queue.put(key, timeout=self.timeout)
                # Increasing waiting time
                time.sleep(self.waiter)
                self.waiter = randint(1, min(self.max_wait, self.waiter * 4))
                logger.debug(f"Next waiting time {self.waiter}s.")
            else:
                # Preparing copy task
                if (self.dst_bucket_role):
                    logger.debug(
                        f"dst_bucket_role:  {self.dst_bucket_role}")
                    self.s3_dst_sts = self._session.client('sts')
                    self.s3_dst_assume_role_object = self.s3_dst_sts.assume_role(
                        RoleArn=self.dst_bucket_role, RoleSessionName="DST_BUCKET_ROLE", DurationSeconds=3600)
                    self.dst_credentials = self.s3_dst_assume_role_object['Credentials']
                    self.dst_tmp_access_key = self.dst_credentials['AccessKeyId']
                    self.dst_tmp_secret_key = self.dst_credentials['SecretAccessKey']
                    self.dst_tmp_security_token = self.dst_credentials['SessionToken']
                    self.dst_session = boto3.session.Session(
                        aws_access_key_id=self.dst_tmp_access_key,
                        aws_secret_access_key=self.dst_tmp_secret_key, aws_session_token=self.dst_tmp_security_token
                    )
                    s3_dst = self.dst_session.resource('s3')

                    logger.info("copy with assume role of dst_bucket")
                    dst_obj = s3_dst.Object(self.dst_bucket, key)
                else:
                    logger.info("copy without assume role of dst_bucket")
                    dst_obj = s3.Object(self.dst_bucket, key)

                cp_src = {'Bucket': self.src_bucket, 'Key': key}
                try:
                    logger.info(f"{self.name} copying {key}")
                    dst_obj.copy(cp_src, Config=self._transfer_mgr)
                except ClientError as exc:
                    try:
                        error_code = exc.response['Error']['Code']
                        if 'SlowDown' in error_code:
                            logger.warning("SlowDown occurs. "
                                           f"Waiting for {self.waiter:.0f}s"
                                           )
                            logger.debug(f"{exc.response}\n Key {key}")
                            put_metric('SlowDown', 1, self.config)
                        elif 'InternalError' in error_code:
                            logger.warning("InternalError occurs. Waiting for "
                                           f"{self.waiter:.0f}s")
                            put_metric(self.cw_metric_name, 1, self.config)
                            logger.debug(f"{exc.response}\n Key {key}")
                        else:
                            logger.error(
                                f"{exc.response}\n Key {key} \n Exception: {exc}")
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
                        logger.debug("Error occured sleeping for "
                                     f"{self.waiter}s.")
                        time.sleep(self.waiter)
                        # Increase maximum of waiting time
                        self.waiter = randint(
                            1, min(self.max_wait, self.waiter * 4))
                        logger.debug(f"Next waiting time {self.waiter}s.")
                    else:
                        logger.error(f"Put {key} back to queue.")
                        self.restore_queue.put(key, timeout=self.timeout)
                        logger.debug("Error occured sleeping for "
                                     f"{self.waiter}s.")
                        time.sleep(self.waiter)
                        # Increase maximum of waiting time
                        self.waiter = randint(
                            1, min(self.max_wait, self.waiter * 4))
                        logger.debug(f"Next waiting time {self.waiter}s.")
                except ConnectionRefusedError as exc:
                    logger.exception(f"Waiting for {self.waiter:.0f}s.\n"
                                     f"Put {key} back to queue.\n"
                                     "Maybe to many connections?")
                    put_metric(self.cw_metric_name, 1, self.config)
                    self.restore_queue.put(key, timeout=self.timeout)
                    logger.debug(f"Error occured sleeping for {self.waiter}s.")
                    time.sleep(self.waiter)
                    self.waiter = randint(
                        1, min(self.max_wait, self.waiter * 4))
                    logger.debug(f"Next waiting time {self.waiter}s.")
                except EndpointConnectionError as exc:
                    logger.warning("EndpointConnectionError.\n"
                                   f"Waiting for {self.waiter:.0f}s.\n"
                                   f"Put {key} back to queue.\n")
                    put_metric(self.cw_metric_name, 1, self.config)
                    self.restore_queue.put(key, timeout=self.timeout)
                    logger.debug(f"Error occured sleeping for {self.waiter}s.")
                    time.sleep(self.waiter)
                    self.waiter = randint(
                        1, min(self.max_wait, self.waiter * 4))
                    logger.debug(f"Next waiting time {self.waiter}s.")
                except Exception as exc:
                    logger.exception("Unhandeld exception occured.\n "
                                     f"Put {key} back to queue.")
                    put_metric(self.cw_metric_name, 1, self.config)
                    self.restore_queue.put(key, timeout=self.timeout)
                    logger.debug(f"Error occured sleeping for {self.waiter}s.")
                    self.waiter = randint(
                        1, min(self.max_wait, self.waiter * 4))
                    logger.debug(f"Next waiting time {self.waiter}s.")
                else:
                    logger.info(f"{self.name} copied {key}")
                    # Reduce waiting time
                    self.waiter = max(round(self.waiter * 0.8), 1)
                    logger.debug(f"Reduced waiting time to {self.waiter}s.")

    def _get_storage_class(self, s3_client, bucket, key):
        """Definition will return StorageClass and OngoingReques
        Args:
            s3_client (client): S3 client object needed to make requests to S3.
            bucket (str): Bucket where the objects are stored.
            key (str): Key in bucket.
        Returns:
            [dict]: (StorageClasse, OngoingRequest)
        """
        try:
            obj = s3_client.Object(bucket, key)
            storage_class = obj.storage_class
            ongoing_req = obj.restore
        except ClientError as exc:
            try:
                error_code = exc.response['Error']['Code']
                if 'SlowDown' in error_code:
                    logger.warning("SlowDown occurs. Waiting for"
                                   f" {self.waiter:.2}s")
                    time.sleep(self.waiter)
                    # Increase maximum of waiting time
                    self.waiter = randint(
                        1, min(self.max_wait, self.waiter * 4))
                    logger.debug(f"{exc.response}\n Key {key}")
                    put_metric('SlowDown', 1, self.config)
                elif 'InternalError' in error_code:
                    logger.warning("InternalError occurs. Waiting for "
                                   f"{self.waiter:.2}s")
                    time.sleep(self.waiter)
                    # Increase maximum of waiting time
                    self.waiter = randint(
                        1, min(self.max_wait, self.waiter * 4))
                    logger.debug(f"{exc.response}\n Key {key}")
                    put_metric(self.cw_metric_name, 1, self.config)
                else:
                    logger.error(f"{exc.response}\n Key {key}")
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
                logger.debug(f"Error occured sleeping for {self.waiter:.2}s.")
                time.sleep(self.waiter)
                # Increase maximum of waiting time
                self.waiter = randint(1, min(self.max_wait, self.waiter * 4))
                logger.debug(f"Next waiting time {self.waiter}s.")
            else:
                logger.error(f"Put {key} back to queue.")
                self.restore_queue.put(key, timeout=self.timeout)
                time.sleep(self.waiter)
                # Increase maximum of waiting time
                self.waiter = randint(1, min(self.max_wait, self.waiter * 4))
                logger.debug(f"Next waiting time {self.waiter}s.")
        except EndpointConnectionError as exc:
            logger.warning("EndpointConnectionError.\n"
                           f"Waiting for {self.waiter}s.\n"
                           f"Put {key} back to queue.\n")
            put_metric(self.cw_metric_name, 1, self.config)
            self.restore_queue.put(key, timeout=self.timeout)
            logger.debug(f"Error occured sleeping for {self.waiter}s.")
            time.sleep(self.waiter)
            # Increase maximum of waiting time
            self.waiter = randint(1, min(self.max_wait, self.waiter * 4))
            logger.debug(f"Next waiting time {self.waiter}s.")
        except Exception as exc:
            logger.exception("Unhandeld exception occured.\n "
                             f"Put {key} back to queue.")
            put_metric(self.cw_metric_name, 1, self.config)
            self.restore_queue.put(key, timeout=self.timeout)
            logger.debug(f"Error occured sleeping for {self.waiter}s.")
            time.sleep(self.waiter)
            # Increase maximum of waiting time
            self.waiter = randint(1, min(self.max_wait, self.waiter * 4))
            logger.debug(f"Next waiting time {self.waiter}s.")
        else:
            logger.info(f"Object {key} has storage class {storage_class}")
            # Reduce waiting time
            self.waiter = max(round(self.waiter * 0.8), 1)
            logger.debug(f"Reduced waiting time to {self.waiter}s.")
            return {
                'StorageClass': storage_class,
                'OngoingRequest': ongoing_req
            }


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
        logger.debug(f"{self.name} restore queue size {restore_queue_size}")
        if restore_queue_size:
            thread_count = min(self.thread_count, restore_queue_size)

            # Start copying S3 objects to destiantion bucket
            # Consume restore_queue until it is empty
            th_lst = list()
            logger.info(f"{self.name} starting {thread_count} threads.")
            for t in range(thread_count):
                th_lst.append(_Restore(
                    self.config,
                    self.restore_queue))
                logger.debug(f"{self.name} {th_lst[t].name} generated.")
                th_lst[t].start()
                logger.debug(f"{self.name} {th_lst[t].name} started.")

            try:
                logger.debug("Checking restore queue size if empty.")
                while not self.restore_queue.empty():
                    logger.info("Restore queue not empty "
                                f"{self.restore_queue.qsize()} keys. "
                                "Waiting for 60s.")
                    time.sleep(60)
            except KeyboardInterrupt:
                logger.info("Exiting...")
                sys.exit(127)

            logger.info(f"{self.name} joining all threads.")
            for t in range(thread_count):
                logger.debug(f"{self.name} waiting for "
                             f"{th_lst[t].name} to be finished.")
                try:
                    while th_lst[t].is_alive():
                        logger.debug(f"In {self.name} {th_lst[t].name} "
                                     "is still alive. Waiting for 60s.")
                        time.sleep(60)
                except KeyboardInterrupt:
                    logger.warning("Exiting...")
                    sys.exit(127)
                else:
                    logger.debug(f"Try joining copy {th_lst[t].name}.")
                    th_lst[t].join(timeout=self.timeout)
                    logger.debug(f"{self.name} {th_lst[t].name} joined.")
            logger.info(f"{self.name} all restore threads finished.")
        else:
            logger.warning(f"No objects to restore for {self.name}!")
