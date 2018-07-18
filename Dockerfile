FROM amazonlinux:2
WORKDIR /app

RUN yum -y update && yum -y install python3

RUN yum clean all
RUN rm -rf /var/cache/yum

COPY s3backuprestore /app/s3backuprestore

COPY *.py /app/
COPY requirements.txt /app/

RUN pip-3 install -r /app/requirements.txt

USER root

ENTRYPOINT ["python3"]
