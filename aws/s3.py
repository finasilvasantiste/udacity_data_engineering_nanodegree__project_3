from aws.AWSResource import AWSResource


def print_s3_bucket_content():
    """
    Prints out the objects contained in an S3 bucket.
    :return:
    """
    s3_resource = AWSResource(resource_name='s3').resource

    bucket = s3_resource.Bucket('udacity-dend')

    for obj in bucket.objects.filter(Prefix="log_data"):
        print(obj)


if __name__ == "__main__":
    print_s3_bucket_content()
