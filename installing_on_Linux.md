# Installing ABOK Loader on a Linux Server

Sometimes installing the packages on a Windows machine can be a bit tricky. 
Here is a quick and easy way to install this repo on a Linux machine. 

Prepare by copying ABOK.gzip and ABOK_media_files.zip to the home directory, e.g. /home/ec2-user/

scp -i ~/.ssh/YOURKEY.pem ABOK.gzip  ec2-user@YOURHOST:/home/ec2-user/

scp -i ~/.ssh/YOURKEY.pem ABOK_media_files.zip ec2-user@YOURHOST:/home/ec2-user/

sudo yum install python36 # red hat style

python3 --version

pip -V

git clone https://github.com/mafux777/Alation_Article

cd Alation_Article/

git checkout abok_loader

pip3 install -r abok_re.txt --user

cp ../ABOK.gzip .

cp ../ABOK_media_files.zip .

python3 abok_loader.py -H http_YOURHOST -u YOURUSER -p YOURPASSWORD
