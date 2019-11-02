# import base64
# import paramiko
#
# #from scp import SCPClient
# from scpclient import WriteDir, Write
from contextlib import *
#
from zipfile import ZipFile
#
#
def extract_files(base_path):
    with closing(ZipFile(base_path + 'ABOK_media_files.zip', 'r')) as myzip:
        x = myzip.extractall()


def list_files(base_path):
    try:
        with closing(ZipFile(base_path + 'ABOK_media_files.zip', 'r')) as myzip:
            return myzip.NameToInfo.keys()
    except:
        return []
#
#
# def secure_copy(host, username, key_filename, local_dir, remote_dir):
#     ssh_client = paramiko.SSHClient()
#     ssh_client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)
#     ssh_client.connect(host, username=username, key_filename=key_filename)
#     with closing(WriteDir(ssh_client.get_transport(), remote_dir)) as scp:
#         scp.send_dir(local_dir, preserve_times=True)
#
#
# if __name__ == "__main__":
#     # extract_files()
#     l = list_files()
#     secure_copy(host='18.218.6.215',
#                 username='ec2-user',
#                 key_filename='/Users/matthias.funke/.ssh/PSPersonMachines.pem',
#                 local_dir=u"media/image_bank/", remote_dir="/mnt/data/site_data/media/image_bank/")
#
