import configparser
from aws.AWSClient import AWSClient
import pandas as pd


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
        self.iam_roles = ['']  # TODO: create redshift user and add to cluster

    def create_cluster(self):
        """
        Creates redshift cluster.
        :return:
        """
        redshift_client = AWSClient(resource='redshift').client

        try:
            redshift_client.create_cluster(ClusterIdentifier=self.cluster_identifier,
                                           ClusterType=self.cluster_type,
                                           NodeType=self.node_type,
                                           MasterUsername=self.user_name,
                                           MasterUserPassword=self.password,
                                           NumberOfNodes=self.number_of_nodes)
            print('+++++ Creating cluster {}... +++++'.format(self.cluster_identifier))
        except Exception as e:
            print(e)
        finally:
            print("+++++ The 'try except' is finished. +++++")

    def delete_cluster(self):
        """
        Deletes redshift cluster.
        :return:
        """
        redshift_client = AWSClient(resource='redshift').client

        try:
            redshift_client.delete_cluster(ClusterIdentifier=self.cluster_identifier,
                                           SkipFinalClusterSnapshot=True)
            print('+++++ Deleteting cluster {}... +++++'.format(self.cluster_identifier))
        except Exception as e:
            print(e)
        finally:
            print("+++++ The 'try except' is finished. +++++")

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
