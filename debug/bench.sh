killall tarantool
rm *.snap
rm m1.log
rm work/m2.log
rm work/*.snap

tarantool master.lua &
tarantool master1.lua &

