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


if __name__ == "__main__":
    rc = RedshiftCluster()
    # rc.create_cluster()

    rc.delete_cluster()
    # rc.describe_cluster()

    # rc.set_cluster_arn()
    print(rc.cluster_arn)
