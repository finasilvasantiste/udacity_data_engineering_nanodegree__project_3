import configparser
from aws.RedshiftCluster import RedshiftCluster
import psycopg2


class DBHandler:
    """ Represent a database handler. """

    def __init__(self, redshiftCluster: RedshiftCluster):
        config = configparser.ConfigParser()
        config.read_file((open(r'dwh.cfg')))

        self.db_name = config.get('REDSHIFT_CLUSTER', 'DB_NAME')
        self.user_name = config.get('REDSHIFT_CLUSTER', 'USER_NAME')
        self.password = config.get('REDSHIFT_CLUSTER', 'PASSWORD')
        self.db_name = config.get('REDSHIFT_CLUSTER', 'DB_NAME')
        self.port = redshiftCluster.get_cluster_port_from_cloud()
        self.address = redshiftCluster.get_cluster_address_from_cloud()
        self.vpc_id = redshiftCluster.get_cluster_vpc_id_from_cloud()
        self.cnx = None

    def get_db_connection(self):
        user = self.user_name
        password = self.password
        address = self.address
        port = int(self.port)
        db_name = self.db_name
        print(address)
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(address,
                                                                                       db_name,
                                                                                       user,
                                                                                       password,
                                                                                       port))
        cur = conn.cursor()


        queries = ['''SELECT * FROM pg_catalog.pg_tables;''']

        for query in queries:
            cur.execute(query)

            conn.commit()
            results = cur.fetchall()
            # for r in results:
                # print(r)

        conn.close()
