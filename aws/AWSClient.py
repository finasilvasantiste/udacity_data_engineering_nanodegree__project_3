import boto3
import configparser


class AWSClient:
    """ Represents an aws client. """

    aws_users = {'admin': 'AWS_CREDS_ADMIN'}

    def __init__(self, resource):
        self.aws_user = 'admin'
        session = self.get_boto3_session()
        self.client = session.client(resource)

    def get_boto3_session(self):
        """
        Returns authenticated boto3 session.
        :return:
        """
        aws_access_key_id, aws_secret_access_key, aws_region = self.get_aws_credentials()

        session = boto3.Session(aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key,
                                region_name=aws_region)

        return session

    def get_aws_credentials(self):
        """
        Returns aws credentials for given user.
        :return: three strings
        """
        config = configparser.ConfigParser()
        config.read_file((open(r'dwh.cfg')))

        aws_access_key_id = config.get(self.aws_users[self.aws_user], 'AWS_ACCESS_KEY_ID')
        aws_secret_access_key = config.get(self.aws_users[self.aws_user], 'AWS_SECRET_ACCESS_KEY')
        aws_region = config.get(self.aws_users[self.aws_user], 'AWS_REGION')

        return aws_access_key_id, aws_secret_access_key, aws_region
