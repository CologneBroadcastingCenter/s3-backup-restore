SHELL = /bin/sh

.DEFAULT_GOAL := login

login:
		`aws --profile ${PROFILE} ecr get-login --no-include-email --region eu-central-1`

build:
		docker build --no-cache -t cbc/clouds/s3-backup-restore .

staging: build
		docker tag cbc/clouds/s3-backup-restore:latest 505022722956.dkr.ecr.eu-central-1.amazonaws.com/cbc/clouds/s3-backup-restore:staging
		docker push 505022722956.dkr.ecr.eu-central-1.amazonaws.com/cbc/clouds/s3-backup-restore:staging

prod: build
		docker tag cbc/clouds/s3-backup-restore:latest 505022722956.dkr.ecr.eu-central-1.amazonaws.com/cbc/clouds/s3-backup-restore:prod
		docker push 505022722956.dkr.ecr.eu-central-1.amazonaws.com/cbc/clouds/s3-backup-restore:prod

deploy: staging prod
