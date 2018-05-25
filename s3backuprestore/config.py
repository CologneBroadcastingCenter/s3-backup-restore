import boto3


class Config(object):
    def __init__(self, src_bucket, dst_bucket, access_key=None,
                 secret_key=None, token=None, timeout=None, last_modified=None,
                 cw_namespace=None, cw_dimension_name=None, profile_name=None,
                 region=None):
        self._access_key = access_key
        self._secret_key = secret_key
        self._token = token
        self._src_bucket = src_bucket
        self._dst_bucket = dst_bucket
        self._timeout = timeout or 120
        self._last_modified = last_modified or 48
        self._cw_namespace = cw_namespace or 'BackupRecovery'
        self._cw_dimension_name = cw_dimension_name or 'Dev'
        self._profile_name = profile_name
        self._region = region or 'eu-central-1'

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

    def boto3_session(self):
        frz_creds = None
        if self._access_key and self._secret_key and self._token:
            session = boto3.session.Session(
                aws_access_key_id=self._access_key,
                aws_secret_access_key=self._secret_key,
                aws_session_token=self._token)
        elif self._profile_name:
            session = boto3.session.Session(
                profile_name=self._profile_name,
                region_name=self._region)
            frz_creds = session.get_credentials().get_frozen_credentials()
        else:
            session = boto3.session.Session(region_name=self._region)
            frz_creds = session.get_credentials().get_frozen_credentials()

        if frz_creds:
            self._access_key = frz_creds.access_key
            self._secret_key = frz_creds.secret_key
            self._token = frz_creds.token

        return session

    def get_credentials(self):
        session = self.boto3_session()
        frz_creds = session.get_credentials().get_frozen_credentials()
        self._access_key = frz_creds.access_key
        self._secret_key = frz_creds.secret_key
        self._token = frz_creds.token
        return frz_creds
