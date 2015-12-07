curl http://tarantool.org/dist/public.key | sudo apt-key add -
echo "deb http://tarantool.org/dist/master/ubuntu/ `lsb_release -c -s` main" | sudo tee -a /etc/apt/sources.list.d/tarantool.list
sudo apt-get update > /dev/null
sudo apt-get -q -y install tarantool tarantool-dev tarantool-pool

git submodule update --init --recursive
pip install -r test-run/requirements.txt
pip install git+https://github.com/tarantool/tarantool-python.git
cd test/ && python test-run.py --force
