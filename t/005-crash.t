#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More tests    => 19;
use Encode qw(decode encode);


BEGIN {
    # Подготовка объекта тестирования для работы с utf8
    my $builder = Test::More->builder;
    binmode $builder->output,         ":utf8";
    binmode $builder->failure_output, ":utf8";
    binmode $builder->todo_output,    ":utf8";

    use_ok 'File::Spec::Functions', 'catfile', 'rel2abs';
    use_ok 'File::Basename', 'dirname', 'basename';
    use_ok 'DR::Tarantool::StartTest';
    use_ok 'Coro';
    use_ok 'Coro::AnyEvent';
    use_ok 'DR::Tarantool', 'coro_tarantool';
}

use feature 'state';

my $lua = rel2abs catfile dirname(dirname __FILE__), 'init.lua';
my $cfg = rel2abs catfile dirname(dirname __FILE__), 'tarantool.cfg';
ok -r $lua, "-r $lua";
ok -r $cfg, "-r $cfg";

my $sh1 =
    run DR::Tarantool::StartTest(script_dir => dirname($lua), cfg => $cfg);

diag $sh1->log unless
ok $sh1->started, 'Shard1 is started';

my $sh2 =
    run DR::Tarantool::StartTest(script_dir => dirname($lua), cfg => $cfg);

diag $sh2->log unless
    ok $sh2->started, 'Shard1 is started';


my $spaces = {
    0 => {
        name => 'test',
        fields  => [ { type => 'NUM', name => 'id' }, qw(f1 f2) ],
        indexes => {
            0 => {
                name => 'id',
                fields => [ 'id' ]
            }
        }
    }
};

my $t1 = eval { coro_tarantool port => $sh1->primary_port, spaces => $spaces };
diag $sh1->log unless
    ok $t1, 'Connector to shard1';
ok $t1->ping, 'ping the first shard';

my $t2 = eval { coro_tarantool port => $sh2->primary_port, spaces => $spaces };
diag $sh2->log unless
    ok $t2, 'Connector to shard2';
ok $t2->ping, 'ping the second shard';



is $t1->call_lua('box.shard.schema:valid' => [],
    fields => [ { type => 'NUM' } ])->raw(0), 0,
        '!box.shard1.schema:valid()';
is $t2->call_lua('box.shard.schema:valid' => [],
    fields => [ { type => 'NUM' } ])->raw(0), 0,
        '!box.shard2.schema:valid()';

{
    my $done = $t1->call_lua('box.dostring', [
            qq/
                return box.shard.schema.config({
                    this = 1,
                    list = {
                        { '127.0.0.1', @{[ $sh1->primary_port ]}, },
                        { '127.0.0.1', @{[ $sh2->primary_port ]}, },
                    }
                })
            /,
        ],
        fields => [ { type => 'NUM' } ]
    )->raw(0);
    
    is $done, 1, 'shard 1 is configured';
}

{
    my $done = $t2->call_lua('box.dostring', [
            qq/
                return box.shard.schema.config({
                    this = 2,
                    list = {
                        { '127.0.0.1', @{[ $sh1->primary_port ]}, },
                        { '127.0.0.1', @{[ $sh2->primary_port ]}, },
                    }
                })
            /,
        ],
        fields => [ { type => 'NUM' } ]
    )->raw(0);
    
    is $done, 1, 'shard 1 is configured';
}

eval {
    $t1->call_lua('box.dostring', [
        'box.shard.schema.box[2]:call("box.shard.raise", "12345",
            "test error message")'
    ]);
};

like $@, qr{test error message}, 'error text';

END {
    note '=========================================';
    note $sh1->log;
    note '=========================================';
    note $sh2->log;
    note '=========================================';
}
