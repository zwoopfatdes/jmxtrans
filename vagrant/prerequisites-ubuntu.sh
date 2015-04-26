#!/bin/sh

apt-get update
apt-get dist-upgrade -y
apt-get install -y maven openjdk-7-jdk git

if [ ! -f /usr/local/bin/bats ]
then
  git clone https://github.com/sstephenson/bats.git
  cd bats
  ./install.sh /usr/local
fi
