from sqlalchemy import create_engine
import configparser
from aws.RedshiftCluster import RedshiftCluster


class DBHandler:

    def __init__(self, redshiftCluster: RedshiftCluster):
        config = configparser.ConfigParser()
        config.read_file((open(r'dwh.cfg')))

        self.db_name = config.get('REDSHIFT_CLUSTER', 'DB_NAME')
        self.user_name = config.get('REDSHIFT_CLUSTER', 'USER_NAME')
        self.password = config.get('REDSHIFT_CLUSTER', 'PASSWORD')
        self.db_name = config.get('REDSHIFT_CLUSTER', 'DB_NAME')
        self.port = redshiftCluster.get_cluster_port_from_cloud()
        self.address = redshiftCluster.get_cluster_address_from_cloud()
        self.cnx = None

    def get_db_connection(self):
        user = self.user_name
        password = self.password
        address = self.address
        port = self.port
        db_name = self.db_name
        cnx_string = 'postgresql://{}:{}@{}:{}/{}'.format(user, password, address, port, db_name)
        engine = create_engine(cnx_string)

        # print(cnx_string)
