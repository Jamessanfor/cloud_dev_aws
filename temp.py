import boto3


if __name__ == "__main__":

    s3 = boto3.client('s3')
    s3.upload_file('1612306368338', 'usu-cs5250-blue-requests','1612306368338')
    
