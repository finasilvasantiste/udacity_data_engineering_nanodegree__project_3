import configparser
from aws.AWSClient import AWSClient


class RedshiftCluster:
    """ Represents a redshift cluster."""

    def __init__(self):
        config = configparser.ConfigParser()
        config.read_file((open(r'dwh.cfg')))

        self.user_name = config.get('REDSHIFT_CLUSTER', 'USER_NAME')
        self.password = config.get('REDSHIFT_CLUSTER', 'PASSWORD')
        self.cluster_identifier = config.get('REDSHIFT_CLUSTER', 'CLUSTER_IDENTIFIER')
        self.node_type = 'dc2.Large'
        self.number_of_nodes = 4

    def create_cluster(self):
        redshift_client = AWSClient(resource='redshift').client
        redshift_client.create_cluster(ClusterIdentifier=self.cluster_identifier,
                                       NodeType=self.node_type,
                                       MasterUsername=self.user_name,
                                       MasterUserPassword=self.password,
                                       NumberOfNodes=self.number_of_nodes)
