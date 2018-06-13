#!/bin/bash

set -euxo pipefail  # Strict shell

curl -s https://packagecloud.io/install/repositories/tarantool/1_7/script.deb.sh | sudo bash
sudo apt-get update > /dev/null
sudo apt-get -q -y install tarantool tarantool-dev libmsgpuck-dev

git submodule update --init --recursive
pip install -r test-run/requirements.txt
pip install git+https://github.com/tarantool/tarantool-python.git
sudo chown $USER:$USER /var/lib/tarantool/
cmake .
make test
