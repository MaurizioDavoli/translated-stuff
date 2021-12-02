"""testing actions script"""
import boto3
import os

print("I ran thanks to a GitHub action")


def read_from_s3():
    """read a random file from s3"""
    print("prima")
    s3_client = boto3.client('s3')

    with open("document.bin", "wb") as file:
        s3_client.download_file('test-translated-bucket', '1.txt', 'document.bin')
        file.close()

    with open("document.bin", "rb") as file2:
        cont = file2.read().decode("ascii")
        file2.close()
    os.remove("document.bin")
    print(cont)
    print("dopo")


read_from_s3()
