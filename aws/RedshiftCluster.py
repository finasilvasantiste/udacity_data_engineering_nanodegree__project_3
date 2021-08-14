import configparser
from aws.AWSClient import AWSClient
import pandas as pd
import json
import time


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
        self.db_name = config.get('REDSHIFT_CLUSTER', 'DB_NAME')
        self.iam_role_name = config.get('REDSHIFT_CLUSTER', 'IAM_ROLE_NAME')
        self.iam_role_arn = None
        self.address = None
        self.port = None

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
            print('+++++ Threw Exception +++++')
            print(e)

        try:
            print('+++++ Attaching IAM Role Policy... +++++')

            iam_client.attach_role_policy(
                RoleName=self.iam_role_name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
        except Exception as e:
            print('+++++ Threw Exception +++++')
            print(e)

        try:
            print('+++++ Saving IAM Role ARN... +++++')

            self.iam_role_arn = iam_client.get_role(RoleName=self.iam_role_name)['Role']['Arn']
        except Exception as e:
            print('+++++ Threw Exception +++++')
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
            self.iam_role_arn = None
        except Exception as e:
            print('+++++ Threw Exception +++++')
            print(e)

    def create_all_resources(self):
        """
        Creates all necessary Redshift resources.
        :return:
        """
        self.create_iam_role()
        self.create_cluster()

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
                                           IamRoles=[self.iam_role_arn],
                                           DBName=self.db_name)
            self.set_cluster_address()
        except Exception as e:
            print('+++++ Threw Exception +++++')
            print(e)

    def delete_all_resources(self):
        """
        Delete all resources.
        :return:
        """
        self.delete_iam_role()
        self.delete_cluster()

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
            self.address = None
        except Exception as e:
            print('+++++ Threw Exception +++++')
            print(e)

    def get_cluster_description(self):
        """
        Returns cluster description. Queries aws to get the arn.
        :return:
        """
        redshift_client = AWSClient(resource='redshift').client
        cluster_description = redshift_client.describe_clusters(ClusterIdentifier=self.cluster_identifier)['Clusters'][0]

        return cluster_description

    def get_cluster_status(self):
        """
        Returns cluster status.
        :return: string containing cluster status
        """
        cluster_description = self.get_cluster_description()
        cluster_status = cluster_description['ClusterStatus']

        return cluster_status

    def get_cluster_address_from_cloud(self):
        """
        Returns cluster address. Queries aws to get the arn.
        :return:
        """
        cluster_description = self.get_cluster_description()
        arn = cluster_description['Endpoint']['Address']

        return arn

    def get_cluster_port_from_cloud(self):
        """
        Returns cluster port. Queries aws to get the arn.
        :return:
        """
        cluster_description = self.get_cluster_description()
        port = cluster_description['Endpoint']['Port']

        return port

    def is_available(self):
        """
        Returns whether cluster status is available.
        :return: boolean
        """
        cluster_status = self.get_cluster_status()

        return cluster_status == 'available'

    def set_cluster_address(self):
        """
        Sets the cluster address.
        :return:
        """
        while not self.is_available():
            print('+++++ Cluster is not available yet. '
                  'Waiting 10 seconds before checking the cluster status again. +++++')

            time.sleep(10)

        print('+++++ Cluster is available. +++++')

    def describe_cluster(self):
        """
        Describes redshift cluster.
        :return:
        """
        try:
            print('+++++ Describing cluster: +++++')
            cluster_description = self.get_cluster_description()
            print(RedshiftCluster.prettyRedshiftProps(cluster_description))
        except Exception as e:
            print('+++++ Threw Exception +++++')
            print(e)

    # copy-pasted this handy method from a jupyter notebook used in the lecture.
    @staticmethod
    def prettyRedshiftProps(props):
        """
        Formats given props in a way that is easier to read.
        :param props: data to format
        :return: df with formatted data
        """
        pd.set_option('display.max_colwidth', None)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint",
                      "NumberOfNodes", 'VpcId']
        x = [(k, v) for k, v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])
