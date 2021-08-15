import configparser
from aws.RedshiftCluster import RedshiftCluster
import psycopg2
import boto3
from aws.AWSClient import AWSClient

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

        aws_access_key_id, aws_secret_access_key, aws_region = AWSClient.get_aws_credentials()
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_region = aws_region

    def get_db_connection(self):

        ec2 = boto3.resource('ec2',
                             region_name=self.aws_region,
                             aws_access_key_id=self.aws_access_key_id,
                             aws_secret_access_key=self.aws_secret_access_key
                             )
        try:
            vpc = ec2.Vpc(id=self.vpc_id)
            defaultSg = list(vpc.security_groups.all())[-1]  # Using default group.
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


        queries = ['SELECT * FROM pg_catalog.pg_tables;']

        for query in queries:
            cur.execute(query)

            conn.commit()
            results = cur.fetchall()
            for r in results:
                print(r)

        conn.close()