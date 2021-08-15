import configparser
from abc import ABC


class AWSClient(ABC):
    """ Represents an aws client. """

    aws_users = {'admin': 'AWS_CREDS_ADMIN'}

    @classmethod
    def get_aws_credentials(cls):
        """
        Returns aws credentials for admin user.
        :return: three strings
        """
        config = configparser.ConfigParser()
        config.read_file((open(r'dwh.cfg')))
        aws_user = cls.aws_users['admin']

        aws_access_key_id = config.get(aws_user, 'AWS_ACCESS_KEY_ID')
        aws_secret_access_key = config.get(aws_user, 'AWS_SECRET_ACCESS_KEY')
        aws_region = config.get(aws_user, 'AWS_REGION')

        return aws_access_key_id, aws_secret_access_key, aws_region