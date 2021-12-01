"""future implementation tests for main project"""
import boto3
import botocore.exceptions


def read_from_bucket():
    """read file from bucket"""
    s3_client = boto3.client('s3')

    with open("document.bin", "rb") as file:
        s3_client.download_file('test-translated-bucket', '0.txt', 'document.bin')
        cont = file.read().decode("ascii")
        print(cont)
        file.close()


def upload_and_wait():
    """wait before starting a new update"""
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    print(str(s3_client.upload_file('document.bin', 'test-2-trsltd', 'document.bin')))
    loaded = False
    while not loaded:
        try:
            s3_resource.Object('test-2-trsltd', 'document.bin').load()
        except botocore.exceptions.ClientError as exc:
            if exc.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                print("Something else has gone wrong.")
        else:
            print("taaaapppooo")
            loaded = True


print('\n*\n')
read_from_bucket()
print('\n*\n')
upload_and_wait()
print('\n*\n')
