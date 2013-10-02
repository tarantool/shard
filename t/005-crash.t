#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More tests    => 15;
use Encode qw(decode encode);


BEGIN {
    use_ok 'File::Spec::Functions', 'catfile', 'rel2abs';
    use_ok 'File::Basename', 'dirname', 'basename';
    use_ok 'DR::Tarantool::StartTest';
    use_ok 'Coro';
    use_ok 'Coro::AnyEvent';
    use_ok 'DR::Tarantool', 'coro_tarantool';
}

use feature 'state';

my $cfg = rel2abs catfile dirname(dirname __FILE__), 'tarantool.cfg';
ok -r $cfg, "-r $cfg";

my $sh1 = run DR::Tarantool::StartTest(cfg => $cfg);

diag $sh1->log unless ok $sh1->started, 'Shard1 is started';


my $t1 = eval { coro_tarantool port => $sh1->primary_port };
diag $sh1->log unless
    ok $t1, 'Connector to shard1';
ok $t1->ping, 'ping the first shard';

eval {
    $t1->call_lua('box.dostring', [qq{
        box.raise(12345, "test error message")
    }]);
};

like $@, qr{test error message}, 'error message';
