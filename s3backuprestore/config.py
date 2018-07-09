import boto3
from boto3.s3.transfer import TransferConfig


class Config(object):
    def __init__(self, src_bucket, dst_bucket, access_key=None,
                 secret_key=None, token=None, timeout=120, last_modified=48,
                 extra_args=None, cw_namespace='BackupRecovery',
                 cw_dimension_name='Dev', profile_name=None,
                 region='eu-central-1', s3_transfer_manager_conf=None):
        """This class provides an easy to use configuration interface.
        This object is used by all classes of this module.

        This object returns a default configuration, you only have to specify
        source and destination buckets everything else ist pre configured with
        sane defaults. So normaly you do not have to change those values.

        Args:
            src_bucket (str): Name of the source bucket. e.g. src-bucket
            dst_bucket (str): Name of the destination bucket. e.g. dest-bucket
            access_key (str, optional): Defaults to None. AWS Access Key,
            if you do not use an IAMInstanceProfile you can feed this object
            with an access key. Be aware if you do so, you also have to specify
            and aws secret access key.
            secret_key (str, optional): Defaults to None. Corresponding secret
            key see access_key.
            token (str, optional): Defaults to None. Do not use this.
            It will only be usable within the object itself and it is
            implemented for later purpose.
            timeout (int, optional): Defaults to 120. Timeout if a blocking
            mechanism is active.
            last_modified (int, optional): Defaults to None. Hours since now
            back to the past to check objects if they are where modified.
            cw_namespace (str, optional): Defaults to None. Cloudwatch
            namespace.
            cw_dimension_name (str, optional): Defaults to None. Cloudwatch
            dimension name.
            profile_name (str, optional): Defaults to None. You can specify
            your profile name so that this will be used. Please be aware using
            a profile with MFA enabled and the token gets invalid, you will not
            beeing asked to revalidate your token. Instead the session expires
            and you will see lots of errrors. Sorry for that, its a limitation
            of boto3.
            region (str, optional): Defaults to None. AWS regions.
            s3_transfer_manager_conf (boto3.s3.transfer.TransferConfig,
            optional): Defaults to None. Configuraion object for
            boto3.s3.copy() method. See: http://boto3.readthedocs.io/en/latest/
            reference/customizations/s3.html#boto3.s3.transfer.TransferConfig
        """

        self._access_key = access_key
        self._secret_key = secret_key
        self._token = token
        self._src_bucket = src_bucket
        self._dst_bucket = dst_bucket
        self._timeout = timeout
        self._last_modified = last_modified
        self._extra_args = extra_args
        self._cw_namespace = cw_namespace
        self._cw_dimension_name = cw_dimension_name
        self._profile_name = profile_name
        self._region = region
        self._s3_transfer_manager_conf = s3_transfer_manager_conf

    @property
    def access_key(self):
        if self._access_key:
            return self._access_key
        else:
            self.get_credentials()
            return self._access_key

    @property
    def secret_key(self):
        if self._secret_key:
            return self._secret_key
        else:
            self.get_credentials()
            self._secret_key

    @property
    def token(self):
        if self._token:
            return self._token
        else:
            self.get_credentials()

    @property
    def src_bucket(self):
        return self._src_bucket

    @src_bucket.setter
    def src_bucket(self, bucket):
        self._src_bucket = bucket

    @property
    def dst_bucket(self):
        return self._dst_bucket

    @dst_bucket.setter
    def dst_bucket(self, bucket):
        self._dest_bucket = bucket

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, timeout):
        self._timeout = timeout

    @property
    def last_modified(self):
        return self._last_modified

    @last_modified.setter
    def last_modified(self, last_modified):
        self._last_modified = last_modified

    @property
    def profile_name(self):
        return self._profile_name

    @profile_name.setter
    def profile_name(self, profile_name):
        self._profile_name = profile_name
        self._access_key = None
        self._secret_key = None
        self._token = None

    @property
    def region(self):
        return self._region

    @region.setter
    def region(self, region):
        self._region = region

    @property
    def extra_args(self):
        return self._extra_args

    @extra_args.setter
    def extra_args(self, **kwargs):
        self._extra_args = kwargs

    @property
    def cw_namespace(self):
        return self._cw_namespace

    @cw_namespace.setter
    def cw_namespace(self, cw_namespace):
        self._cw_namespace = cw_namespace

    @property
    def cw_dimension_name(self):
        return self._cw_dimension_name

    @cw_dimension_name.setter
    def cw_dimension_name(self, cw_dimension_name):
        self._cw_dimension_name = cw_dimension_name

    @property
    def s3_transfer_manager_conf(self):
        return self._s3_transfer_manager_conf

    @s3_transfer_manager_conf.setter
    def s3_transfer_manager_conf(self, **kwargs):
        self._s3_transfer_manager_conf = kwargs

    def s3_transfer_manager(self):
        if self._s3_transfer_manager_conf:
            conf = TransferConfig(**self._s3_transfer_manager_conf)
        else:
            conf = TransferConfig()

        return conf

    # This definition is needed because boto3.session.Session() objects
    # are not pickable, therefore we need a functionality that provides
    # a reusable session.
    def boto3_session(self):
        if self._access_key and self._secret_key and self._token:
            session = boto3.session.Session(
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
                aws_session_token=self._token)
        elif self._profile_name:
            session = boto3.session.Session(
                profile_name=self._profile_name,
                region_name=self._region)
        else:
            session = boto3.session.Session(region_name=self._region)

        creds = session.get_credentials()
        self._access_key = creds.access_key
        self._secret_key = creds.secret_key
        self._token = creds.token

        return session

    def get_credentials(self):
        session = self.boto3_session()
        creds = session.get_credentials()
        self._access_key = creds.access_key
        self._secret_key = creds.secret_key
        self._token = creds.token
        return creds
