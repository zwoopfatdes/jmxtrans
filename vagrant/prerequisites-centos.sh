#!/bin/sh

wget -cv http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
rpm -Uvh epel-release-6*.rpm

wget -cv http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo

yum -y update
yum -y install apache-maven rpm-build git

if [[ ! -f /usr/local/bin/bats ]]
then
  git clone https://github.com/sstephenson/bats.git
  cd bats
  ./install.sh /usr/local
fi
