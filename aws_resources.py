import configparser
import boto3

aws_users = {'admin': 'AWS_CREDS_ADMIN',
             'redshift': 'AWS_CREDS_REDSHIFT'}


def get_redshift_cluster_details():
    """
    Returns redshift cluster details.
    :return: three strings
    """
    config = configparser.ConfigParser()
    config.read_file((open(r'dwh.cfg')))

    aws_access_key_id = config.get('REDSHIFT_CLUSTER', 'AWS_ACCESS_KEY_ID')
    aws_secret_access_key = config.get('REDSHIFT_CLUSTER', 'AWS_SECRET_ACCESS_KEY')

    return aws_access_key_id, aws_secret_access_key

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

    return session


def create_s3_bucket():
    """
    Creates an s3 bucket called 'sparkify'.
    :return:
    """
    aws_user = 'admin'
    session = get_boto3_session(aws_user)
    s3_client = session.client('s3')

    s3_client.create_bucket(Bucket='sparkify')


def delete_s3_bucket():
    """
    Deletes s3 bucket called 'sparkify'.
    :return:
    """
    aws_user = 'admin'
    session = get_boto3_session(aws_user)
    s3_client = session.client('s3')

    s3_client.delete_bucket(Bucket='sparkify')


def create_redshift_cluster():
    """
    Creates a redshift cluster called 'sparkify-cluster'.
    :return:
    """
    aws_user = 'admin'
    session = get_boto3_session(aws_user)
    redshift_client = session.client('redshift')

    redshift_client.create_cluster(ClusterIdentifier='sparkify-cluster',
                                   NodeType='dc2.Large')

if __name__ == "__main__":
    # create_s3_bucket()
    delete_s3_bucket()
