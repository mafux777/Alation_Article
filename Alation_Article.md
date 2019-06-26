Additional Tips
==============

The code downloads the images contained in the articles to your local directory. How do you move them to the target 
instance?

```
scp -i ~/.ssh/<somekey> ~/Downloads/media/image_bank/*  ec2-user@<TARGETHOST>:/home/ec2-user/
ssh -i ~/.ssh/<somekey> ec2-user@<TARGETHOST>
cd /mnt/data/site_data/media/image_bank
sudo cp /home/ec2-user/* .
sudo chown alation:alation *
```

There may be more elegant ways, feel free to send a pull request.

