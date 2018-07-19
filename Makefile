SHELL = /bin/sh
.DEFAULT := help

help:
		@echo "This Makefile helps building and deploying new docker container"
		@echo "for backing up and restoring objects from source bucket"
		@echo "to destination bucket Following targets are available."
		@echo ""
		@echo "Target Logging in to ECR:"
		@echo "\tloging"
		@echo ""
		@echo "Target for building docker container:"
		@echo "\tbuild"
		@echo ""
		@echo "Targets to build tagged container and push to ECR:"
		@echo "\tstaging, prod"
		@echo ""
		@echo "Target that will do all the work at once:"
		@echo "\tdeploy"

login:
		`aws --profile $(or $(PROFILE), rin-tvnow-backup-prod) ecr get-login --no-include-email --region eu-central-1`

build:
		docker build --no-cache -t cbc/clouds/s3-backup-restore .

staging: build
		docker tag cbc/clouds/s3-backup-restore:latest 505022722956.dkr.ecr.eu-central-1.amazonaws.com/cbc/clouds/s3-backup-restore:staging
		docker push 505022722956.dkr.ecr.eu-central-1.amazonaws.com/cbc/clouds/s3-backup-restore:staging

prod: build
		docker tag cbc/clouds/s3-backup-restore:latest 505022722956.dkr.ecr.eu-central-1.amazonaws.com/cbc/clouds/s3-backup-restore:prod
		docker push 505022722956.dkr.ecr.eu-central-1.amazonaws.com/cbc/clouds/s3-backup-restore:prod

deploy: staging prod
