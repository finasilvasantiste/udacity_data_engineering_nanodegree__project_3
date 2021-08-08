import configparser
import boto3

aws_users = {'admin': 'AWS_CREDS_ADMIN',
             'redshift': 'AWS_CREDS_REDSHIFT'}


def get_client(resource):
    """
    Returns authenticated client for given resource.
    :param resource: resource
    :return:
    """
    aws_user = 'admin'
    session = get_boto3_session(aws_user)
    s3_client = session.client(resource)

    return s3_client


def get_redshift_cluster_details():
    """
    Returns redshift cluster details.
    :return: three strings
    """
    config = configparser.ConfigParser()
    config.read_file((open(r'dwh.cfg')))

    user_name = config.get('REDSHIFT_CLUSTER', 'USER_NAME')
    password = config.get('REDSHIFT_CLUSTER', 'PASSWORD')

    return user_name, password


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
    Creates an s3 bucket called 'sparkify-fina'.
    :return:
    """
    s3_client = get_client(resource='s3')
    s3_client.create_bucket(Bucket='sparkify-fina')


def delete_s3_bucket():
    """
    Deletes s3 bucket called 'sparkify-fina'.
    :return:
    """
    s3_client = get_client(resource='s3')
    s3_client.delete_bucket(Bucket='sparkify-fina')


def create_redshift_cluster():
    """
    Creates a redshift cluster called 'sparkify-cluster'.
    :return:
    """
    s3_client = get_client(resource='redshift')
    # redshift_client.create_cluster(ClusterIdentifier='sparkify-cluster',
    #                                NodeType='dc2.Large')

    print(s3_client)


if __name__ == "__main__":
    # create_s3_bucket()
    delete_s3_bucket()
