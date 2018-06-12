FROM amazonlinux:latest
WORKDIR /app

RUN yum -y update && yum -y install python36

RUN yum clean all

ADD s3backuprestore/__init__.py /app/s3backuprestore/__init__.py
ADD s3backuprestore/backup.py   /app/s3backuprestore/backup.py
ADD s3backuprestore/compare.py  /app/s3backuprestore/compare.py
ADD s3backuprestore/config.py   /app/s3backuprestore/config.py
ADD s3backuprestore/cw.py       /app/s3backuprestore/cw.py
ADD s3backuprestore/log.py      /app/s3backuprestore/log.py
ADD s3backuprestore/objects.py  /app/s3backuprestore/objects.py
ADD s3backuprestore/restore.py  /app/s3backuprestore/restore.py
ADD s3backuprestore/tagging.py  /app/s3backuprestore/tagging.py

ADD cmd_args.py /app
ADD s3_backup.py /app
#ADD s3_restore.py /app
ADD requirements.txt /app

RUN pip-3.6 install -r /app/requirements.txt

USER root

ENTRYPOINT ["python3"]
