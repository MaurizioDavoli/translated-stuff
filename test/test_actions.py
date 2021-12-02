"""testing actions script"""
import boto3

print("I ran thanks to a GitHub action")


def read_from_s3():
    """read a random file from s3"""
    print("prima")
    s3_client = boto3.client('s3')
    print("dopo")


read_from_s3()
