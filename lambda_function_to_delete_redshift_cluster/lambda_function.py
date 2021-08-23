import json
from aws.RedshiftCluster import RedshiftCluster


def lambda_handler(event, context):
    delete_aws_redshift_resources()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def delete_aws_redshift_resources():
    """
    Deletes all newly created aws redshift resources.
    :return:
    """
    redshift_cluster = RedshiftCluster()
    redshift_cluster.delete_all_resources()
