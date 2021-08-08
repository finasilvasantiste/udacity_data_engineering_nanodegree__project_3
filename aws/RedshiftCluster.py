import configparser
from aws.AWSClient import AWSClient
import pandas as pd
import json


class RedshiftCluster:
    """ Represents a redshift cluster."""

    def __init__(self):
        config = configparser.ConfigParser()
        config.read_file((open(r'dwh.cfg')))

        self.user_name = config.get('REDSHIFT_CLUSTER', 'USER_NAME')
        self.password = config.get('REDSHIFT_CLUSTER', 'PASSWORD')
        self.cluster_identifier = config.get('REDSHIFT_CLUSTER', 'CLUSTER_IDENTIFIER')
        self.cluster_type = 'multi-node'
        self.node_type = 'dc2.Large'
        self.number_of_nodes = 4
        self.iam_role_name = config.get('REDSHIFT_CLUSTER', 'IAM_ROLE_NAME')
        self.iam_role_arn = None

    def create_iam_role(self):
        """
        Creates IAM role for redshift cluster to access S3.
        :return:
        """
        iam_client = AWSClient(resource='iam').client

        try:
            print('+++++ Creating IAM Role... +++++')

            iam_client.create_role(
                Path='/',
                RoleName=self.iam_role_name,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                                    'Effect': 'Allow',
                                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'})
            )
        except Exception as e:
            print(e)

        try:
            print('+++++ Attaching IAM Role Policy... +++++')

            iam_client.attach_role_policy(
                RoleName=self.iam_role_name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
        except Exception as e:
            print(e)

        try:
            print('+++++ Saving IAM Role ARN... +++++')

            self.iam_role_arn = iam_client.get_role(RoleName=self.iam_role_name)['Role']['Arn']
        except Exception as e:
            print(e)

    def delete_iam_role(self):
        """
        Deletes IAM role for redshift cluster to access S3.
        :return:
        """
        iam_client = AWSClient(resource='iam').client

        try:
            print('+++++ Deleting IAM Role ARN... +++++')

            iam_client.detach_role_policy(RoleName=self.iam_role_name,
                                          PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
                                          )
            iam_client.delete_role(RoleName=self.iam_role_name)
        except Exception as e:
            print(e)

    def create_cluster(self):
        """
        Creates redshift cluster.
        :return:
        """
        redshift_client = AWSClient(resource='redshift').client

        try:
            print('+++++ Creating cluster {}... +++++'.format(self.cluster_identifier))

            redshift_client.create_cluster(ClusterIdentifier=self.cluster_identifier,
                                           ClusterType=self.cluster_type,
                                           NodeType=self.node_type,
                                           MasterUsername=self.user_name,
                                           MasterUserPassword=self.password,
                                           NumberOfNodes=self.number_of_nodes,
                                           IamRoles=[self.iam_role_arn])
        except Exception as e:
            print(e)

    def delete_cluster(self):
        """
        Deletes redshift cluster.
        :return:
        """
        redshift_client = AWSClient(resource='redshift').client

        try:
            print('+++++ Deleteting cluster {}... +++++'.format(self.cluster_identifier))

            redshift_client.delete_cluster(ClusterIdentifier=self.cluster_identifier,
                                           SkipFinalClusterSnapshot=True)
        except Exception as e:
            print(e)

    def describe_cluster(self):
        """
        Describes redshift cluster.
        :return:
        """
        redshift_client = AWSClient(resource='redshift').client
        cluster_description = redshift_client.describe_clusters(ClusterIdentifier=self.cluster_identifier)['Clusters'][0]

        print(RedshiftCluster.prettyRedshiftProps(cluster_description))

    # copy-pasted this handy method from a jupyter notebook used in the lecture.
    @staticmethod
    def prettyRedshiftProps(props):
        pd.set_option('display.max_colwidth', None)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                      "NumberOfNodes", 'VpcId']
        x = [(k, v) for k, v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])
