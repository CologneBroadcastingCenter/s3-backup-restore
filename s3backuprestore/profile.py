import os
from .log import logger


class Create():
    def __init__(self, role=None):

        self.awsdir = "/root/.aws"

        if (os.path.exists(self.awsdir)):
            logger.info(f"{self.awsdir} already exists")
            print(f"{self.awsdir} already exists")
        else:
            os.makedirs(self.awsdir)
            logger.info(f"{self.awsdir} created")
            print(f"{self.awsdir} created")

        self.awsconfig = open(f"{self.awsdir}/config", "w")
        self.awscred = open(f"{self.awsdir}/credentials", "w")
        self.awstextconfig = f"[profile Default]\ncredential_source=EcsContainer\n\n[profile target_account]\ncredential_source=EcsContainer\nrole_arn={role}"
        self.awstextcred = f"[Default]\ncredential_source=EcsContainer"
        try:
            self.awsconfig.write(self.awstextconfig)
            self.awsconfig.close()
            self.awscred.write(self.awstextcred)
            self.awscred.close()

        except:
            logger.error(f"{self.awsdir} could not write")
