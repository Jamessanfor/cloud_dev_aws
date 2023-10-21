import boto3

def list_s3_buckets():
    # Create an S3 client
    s3 = boto3.client('s3')

    # Call S3 to list current buckets
    response = s3.list_buckets()

    # Print bucket names
    print("Existing buckets:")
    for bucket in response['Buckets']:
        print(bucket["Name"])

if __name__ == "__main__":
    list_s3_buckets()
