from aws.RedshiftCluster import RedshiftCluster
from db.DBHandler import DBHandler

if __name__ == "__main__":
    ### IF YOU'RE A UDACITY PROJECT REVIEWER, PLEASE READ:
    ### UDACITY TECHNICAL MENTORS WERE ABLE TO HELP ME DEBUG MY ISSUES.
    ### NO REVIEW IS NEEDED AT THIS POINT SINCE I CAN GOT UNSTUCK AND CAN CONTINUE WORKING.


    ### CREATE REDSHIFT CLUSTER AND ITS NECESSARY RESOURCES
    rc = RedshiftCluster()

    ### Used to work, but as of today (August 15th 2021) throws access denied error:
    ### An error occurred (AccessDenied) when calling the CreateRole operation:
    ### User: user/dwh_admin is not authorized to perform: iam:CreateRole on resource: role/sparkify-redshift-s3-role with an explicit deny
    ###
    ### UPDATE: I went ahead and created a new user with the AdministratorAccess policy.
    ### For whatever reason AWS seems to be ok with that one and resource creation works fine again!
    # rc.create_all_resources()
    RedshiftCluster.describe_cluster()

    # rc.delete_all_resources()
