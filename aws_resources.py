from aws.RedshiftCluster import RedshiftCluster
from db.DBHandler import DBHandler


if __name__ == "__main__":
    rc = RedshiftCluster()

    # rc.describe_cluster()

    rc.create_all_resources()
    # rc.delete_all_resources()

    # db = DBHandler(redshiftCluster=rc)

    # db.get_db_connection()
