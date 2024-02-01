# Crawled website
- Jora - done
- Seek - done
- CareerOne - done
- Indeed - 

EC2 script

```
#!/bin/bash
sudo yum update -y
sudo yum install -y docker
sudo service docker start
sudo usermod -aG docker ec2-user
sudo chkconfig docker on
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
sudo yum update -y
sudo yum install -y ruby
sudo yum install -y wget
cd /home/ec2-user   # Change to your home directory or another directory of your choice
wget https://aws-codedeploy-us-west-2.s3.us-west-2.amazonaws.com/latest/install
chmod +x ./install
sudo ./install auto
sudo service codedeploy-agent start
sudo chkconfig codedeploy-agent on
```