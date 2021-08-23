import configparser
from aws.AWSClient import AWSClient

config = configparser.ConfigParser()
config.read_file((open(r'dwh.cfg')))


class RedshiftCluster:
    """ Represents a redshift cluster."""
    cluster_identifier = config.get('REDSHIFT_CLUSTER', 'CLUSTER_IDENTIFIER')

    def __init__(self):
        self.user_name = config.get('REDSHIFT_CLUSTER', 'USER_NAME')
        self.password = config.get('REDSHIFT_CLUSTER', 'PASSWORD')
        self.cluster_identifier = config.get('REDSHIFT_CLUSTER', 'CLUSTER_IDENTIFIER')
        self.cluster_type = 'multi-node'
        self.node_type = 'dc2.Large'
        self.number_of_nodes = 4
        self.db_name = config.get('REDSHIFT_CLUSTER', 'DB_NAME')
        self.iam_role_name = config.get('REDSHIFT_CLUSTER', 'IAM_ROLE_NAME')
        self.iam_role_arn = None


    def delete_iam_role(self):
        """
        Deletes IAM role for redshift cluster to access S3.
        :return:
        """
        iam_client = AWSClient(client_name='iam').client

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
        redshift_client = AWSClient(client_name='redshift').client

        try:
            print('+++++ Deleteting cluster {}... +++++'.format(self.cluster_identifier))

            redshift_client.delete_cluster(ClusterIdentifier=self.cluster_identifier,
                                           SkipFinalClusterSnapshot=True)
        except Exception as e:
            print('+++++ Threw Exception +++++')
            print(e)

    @staticmethod
    def get_cluster_description():
        """
        Returns cluster description. Queries aws to get the arn.
        :return:
        """
        redshift_client = AWSClient(client_name='redshift').client
        cluster_description = redshift_client.describe_clusters(ClusterIdentifier=RedshiftCluster.cluster_identifier)['Clusters'][0]

        return cluster_description

    @staticmethod
    def get_cluster_status():
        """
        Returns cluster status.
        :return: string containing cluster status
        """
        cluster_description = RedshiftCluster.get_cluster_description()
        cluster_status = cluster_description['ClusterStatus']

        return cluster_status

    @staticmethod
    def get_cluster_vpc_id_from_cloud():
        """
        Returns cluster vpc id. Queries aws to get the information.
        :return:
        """
        cluster_description = RedshiftCluster.get_cluster_description()
        vpc_id = cluster_description['VpcId']

        return vpc_id

    @staticmethod
    def get_cluster_address_from_cloud():
        """
        Returns cluster address. Queries aws to get the information.
        :return:
        """
        cluster_description = RedshiftCluster.get_cluster_description()
        arn = cluster_description['Endpoint']['Address']

        return arn

    @staticmethod
    def get_cluster_port_from_cloud():
        """
        Returns cluster port. Queries aws to get the information.
        :return:
        """
        cluster_description = RedshiftCluster.get_cluster_description()
        port = cluster_description['Endpoint']['Port']

        return port


