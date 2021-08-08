from aws.AWSClient import AWSClient
from aws.RedshiftCluster import RedshiftCluster

def create_s3_bucket():
    """
    Creates an s3 bucket called 'sparkify-fina'.
    :return:
    """
    s3_client = AWSClient(resource='s3').client
    s3_client.create_bucket(Bucket='sparkify-fina')


def delete_s3_bucket():
    """
    Deletes s3 bucket called 'sparkify-fina'.
    :return:
    """
    s3_client = AWSClient(resource='s3').client
    s3_client.delete_bucket(Bucket='sparkify-fina')


def create_redshift_cluster():
    """
    Creates a redshift cluster called 'sparkify-cluster'.
    :return:
    """
    # redshift_client = AWSClient(resource='redshift').client
    #
    # redshift_client.create_cluster(ClusterIdentifier='sparkify-cluster',
    #                                NodeType='dc2.Large')
    rc = RedshiftCluster()
    # rc.create_cluster()

    rc.delete_cluster()
    # rc.describe_cluster()

if __name__ == "__main__":
    # create_s3_bucket()
    # delete_s3_bucket()
    create_redshift_cluster()
