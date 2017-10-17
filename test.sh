#!/bin/bash

curl -s https://packagecloud.io/install/repositories/tarantool/1_6/script.deb.sh | sudo bash
sudo apt-get update > /dev/null
sudo apt-get -q -y install tarantool tarantool-dev

git submodule update --init --recursive
sudo pip install -r test-run/requirements.txt
sudo pip install git+https://github.com/tarantool/tarantool-python.git
sudo chown $USER:$USER /var/lib/tarantool/
make test
