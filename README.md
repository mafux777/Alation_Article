# ABOK Loader (Alation Book of Knowledge)

This branch of the code is meant for uploading ABOK to a customer's instance. 

Proceed as follows:

## Download the code
### Option 1: git
git clone https://github.com/mafux777/Alation_Article/

git checkout abok_loader

### Option 2:
Download the code manually from https://github.com/mafux777/Alation_Article/tree/abok_loader


## Check Python 3 is installed
In a command prompt, type python and look at the version number. It should be 3.x

## Check pip3 is installed
Type pip -V 

The output should be similar to:

pip 19.2.3 from c:\program files (x86)\python38-32\lib\site-packages\pip (python 3.8)

## Install the requirements
Type pip install -r abok_re.txt

There should be no error messages. If you run the same command again, the output should be more or less like this:

Requirement already satisfied: beautifulsoup4==4.8.1 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 1)) (4.8.1)

Requirement already satisfied: bs4==0.0.1 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 2)) (0.0.1)

Requirement already satisfied: certifi==2019.9.11 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 3)) (2019.9.11)

Requirement already satisfied: chardet==3.0.4 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 4)) (3.0.4)

Requirement already satisfied: html5lib==1.0.1 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 5)) (1.0.1)

Requirement already satisfied: idna==2.8 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 6)) (2.8)

Requirement already satisfied: numpy==1.17.3 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 7)) (1.17.3)

Requirement already satisfied: pandas==0.25.3 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 8)) (0.25.3)

Requirement already satisfied: python-dateutil==2.8.0 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 9)) (2.8.0)

Requirement already satisfied: pytz==2019.3 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 10)) (2019.3)

Requirement already satisfied: requests==2.22.0 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 11)) (2.22.0)

Requirement already satisfied: six==1.12.0 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 12)) (1.12.0)

Requirement already satisfied: soupsieve==1.9.4 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 13)) (1.9.4)

Requirement already satisfied: urllib3==1.25.6 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 14)) (1.25.6)

Requirement already satisfied: webencodings==0.5.1 in c:\program files (x86)\python38-32\lib\site-packages (from -r abok_re.txt (line 15)) (0.5.1)

## Copy ABOK.gzip and ABOK_media_files.zip
You should obtain these files from your Alation contact

## Run abok_loader
abok_loader.py -H http_yourhost -u yourusername -p yourpassword
  
If you have any questions, please contact your Alation CSM (not Technical Support)
