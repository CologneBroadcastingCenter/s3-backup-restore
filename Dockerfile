FROM amazonlinux:latest
WORKDIR /app

RUN yum -y update && yum -y install python36

RUN yum clean all

COPY s3backuprestore /app/s3backuprestore

COPY *.py /app/
COPY requirements.txt /app/

RUN pip-3.6 install -r /app/requirements.txt

USER root

ENTRYPOINT ["python3"]
