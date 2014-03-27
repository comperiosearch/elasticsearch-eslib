#!/bin/bash


# Add the EPEL yum repo
rpm -Uvh http://download-i2.fedoraproject.org/pub/epel/6/i386/epel-release-6-8.noarch.rpm

# Update and install basic system software
yum -y update
yum -y install gcc gcc-c++ make telnet tree
yum -y install git
yum -y install python-pip perl-CPAN
yum -y install bzip2-devel
yum -y install xz
yum -y install java-1.7.0-openjdk.x86_64
yum -y install npm --enablerepo=epel

# Install Postgresql for Marmotta
yum -y install postgresql postgresql-server
chkconfig postgresql on

npm install -g express
npm install -g supervisor


# Install Python 3.3.4
TMP=$HOME/tmp
PYTHON_VERSION=3.3.4
PYTHON_ARCHIVE=Python-$PYTHON_VERSION
ENDPOINT=http://python.org/ftp/python/$PYTHON_VERSION/$PYTHON_ARCHIVE.tar.xz 
mkdir -p $TMP

echo $ENDPOINT
curl -L $ENDPOINT | tar -xJ -C $TMP
cd $TMP/$PYTHON_ARCHIVE

./configure --prefix=/usr/local --enable-shared LDFLAGS="-Wl,-rpath /usr/local/lib"
make && make altinstall

curl -L https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py > $TMP/ez_setup.py
/usr/local/bin/python3.3 $TMP/ez_setup.py
/usr/local/bin/easy_install-3.3 pip

ln -s /usr/local/bin/python3.3 /usr/bin/python3
ln -s /usr/local/bin/pip3 /usr/bin/pip3
ln -s /usr/local/bin/easy_install-3.3 /usr/bin/easy_install3
# Python packages
pip3 install elasticsearch  # Official Python API for ElasticSearch
pip3 install feedparser     # RSS fetch lib
pip3 install jsonpickle
pip3 install gspread        # Google spreadsheet lib
pip3 install TwitterAPI
pip3 install rdflib
pip3 install jsonpath-rw
easy_install3 SPARQLWrapper
