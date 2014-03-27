elasticsearch-eslib
===================

Python library for document processing for elasticsearch

The scripts assume that python3.3 can be found at ```/usr/bin/python3```.
If you do not have python3 at that location either change the scripts or add [symlink](http://www.unixtutorial.org/2008/02/unix-symlink-example/)

This project uses submodules
Remember to git clone with the recursive option

	git clone --recursive

##Vagrant
The Vagrantfile provided  will install a vm with CentOS 6.4, python3 , elasticsearch 1.1 and most of the necessary bits.

	vagrant up 