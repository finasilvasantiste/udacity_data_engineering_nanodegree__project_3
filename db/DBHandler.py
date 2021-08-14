import configparser
from aws.RedshiftCluster import RedshiftCluster
import psycopg2
import boto3

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
        self.vpc_id = redshiftCluster.get_cluster_vpc_id_from_cloud()
        self.cnx = None

    def get_db_connection(self):

        ec2 = boto3.resource('ec2',
                             region_name="us-east-1",
                             aws_access_key_id='AKIAX44KHN3JRDQIPLX4',
                             aws_secret_access_key='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'
                             )
        try:
            vpc = ec2.Vpc(id=self.vpc_id)
            defaultSg = list(vpc.security_groups.all())[0]
            print(defaultSg)
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(self.port),
                ToPort=int(self.port)
            )
        except Exception as e:
            print(e)
        #
        # user = self.user_name
        # password = self.password
        # address = self.address
        # port = int(self.port)
        # db_name = self.db_name
        # print(address)
        # conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(address,
        #                                                                                db_name,
        #                                                                                user,
        #                                                                                password,
        #                                                                                port))
        # cur = conn.cursor()
        #
        #
        # queries = ['SELECT * FROM pg_catalog.pg_tables;']
        #
        # for query in queries:
        #     cur.execute(query)
        #     conn.commit()
        #
        # conn.close()