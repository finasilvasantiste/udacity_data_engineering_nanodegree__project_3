import configparser
import boto3

aws_users = {'admin': 'AWS_CREDS_ADMIN',
             'redshift': 'AWS_CREDS_REDSHIFT'}


def get_aws_credentials(aws_user):
    """
    Returns aws credentials for given user.
    :param aws_user: aws user
    :return: three strings
    """
    config = configparser.ConfigParser()
    config.read_file((open(r'dwh.cfg')))

    aws_access_key_id = config.get(aws_users[aws_user], 'AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get(aws_users[aws_user], 'AWS_SECRET_ACCESS_KEY')
    aws_region = config.get(aws_users[aws_user], 'AWS_REGION')

    return aws_access_key_id, aws_secret_access_key, aws_region


def get_boto3_session(aws_user):
    """
    Returns authenticated boto3 session.
    :param aws_user: aws user to create session for
    :return:
    """
    aws_access_key_id, aws_secret_access_key, aws_region = get_aws_credentials(aws_user)

    session = boto3.Session(aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=aws_region)

    print(session)

    return session


def create_s3_bucket():
    aws_user = 'admin'
    session = get_boto3_session(aws_user)
    s3_client = session.client('s3')

    s3_client.create_bucket(Bucket='finas-test-bucket')

if __name__ == "__main__":
    result = create_s3_bucket()

    print(result)