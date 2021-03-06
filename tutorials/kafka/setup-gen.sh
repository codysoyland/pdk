#!/bin/bash

set -e

cd $HOME
echo `pwd`
wget https://storage.googleapis.com/golang/go1.10.linux-amd64.tar.gz 2>/dev/null
sudo tar -C /usr/local -xzf go1.10.linux-amd64.tar.gz
sudo chown -R $USER:$USER /usr/local/go
mkdir -p /home/$USER/go/src/github.com/pilosa
mkdir -p /home/$USER/go/bin
export GOPATH=/home/$USER/go
export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin

echo "export GOPATH=/home/$USER/go" >> .profile
echo "export PATH=$PATH:/usr/local/go/bin:$GOPATH/bin" >> .profile

curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

sudo apt-get -y install make git

git clone https://github.com/pilosa/pdk.git $GOPATH/src/github.com/pilosa/pdk
cd $GOPATH/src/github.com/pilosa/pdk
git remote add jaffee https://github.com/jaffee/pdk.git
git fetch jaffee
git checkout jaffee/kafka-tutorial
make install


