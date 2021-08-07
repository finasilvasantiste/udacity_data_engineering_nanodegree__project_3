import configparser
import boto3

aws_users = {'admin': 'AWS_CREDS_ADMIN',
             'redshift': 'AWS_CREDS_REDSHIFT'}


def load_credentials(aws_user):
    """
    Loads aws credentials for given user.
    :param aws_user: aws user
    :return: two strings
    """
    config = configparser.ConfigParser()
    config.read_file((open(r'dwh.cfg')))

    aws_access_key_id = config.get(aws_users[aws_user], 'AWS_ACCESS_KEY_ID')
    aws_secret_access_key_id = config.get(aws_users[aws_user], 'AWS_SECRET_ACCESS_KEY_ID')

    return aws_access_key_id, aws_secret_access_key_id


def create_s3_bucket():
    pass


if __name__ == "__main__":
    load_credentials('admin')
