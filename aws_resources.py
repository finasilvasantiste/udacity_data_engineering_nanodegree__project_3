from aws.RedshiftCluster import RedshiftCluster
from db.DBHandler import DBHandler


if __name__ == "__main__":
    ### CREATE REDSHIFT CLUSTER AND ITS NECESARRY RESOURCES
    rc = RedshiftCluster()

    rc.create_all_resources()
    # rc.describe_cluster()

    # rc.delete_all_resources()


    ### CREATE DB CONNECTION
    # db = DBHandler(redshiftCluster=rc)
    # db.get_db_connection()
