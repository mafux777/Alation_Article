import base64
import paramiko

from scp import SCPClient


def secure_copy(host, username, key_filename, local_dir):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy)
    client.connect(host, username, key_filename)
    # stdin, stdout, stderr = client.exec_command('ls')
    # for line in stdout:
    #     print('... ' + line.strip('\n'))
    # client.close()



    # SCPCLient takes a paramiko transport as an argument
    scp_client = SCPClient(client.get_transport())


    # Uploading the 'test' directory with its content in the
    # '/home/user/dump' remote directory
    scp_client.put(local_dir, recursive=True, remote_path='~')

    scp_client.close()
    client.close()
