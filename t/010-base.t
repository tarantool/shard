#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More tests    => 72;
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
    use_ok 'DR::Tarantool', ':constant';
}

use feature 'state';

my $lua = rel2abs catfile dirname(dirname __FILE__), 'init.lua';
my $cfg = rel2abs catfile dirname(dirname __FILE__), 'tarantool.cfg';
ok -r $lua, "-r $lua";
ok -r $cfg, "-r $cfg";


sub config($@) {
    my ($tnt, %opts) = @_;

    my $str = join ",\n", map { "$_ = $opts{$_}" } keys %opts;

    my $res = eval {
        $tnt->call_lua('box.dostring' =>
            [ "return box.shard.schema.config({ $str })" ]
        );
    };

    diag $@ if $@;
    return $res;
}

sub load_func($$$) {
    my ($tnt, $where, $body) = @_;
    config $tnt, $where => $body;
}

my $sh1 =
    run DR::Tarantool::StartTest(script_dir => dirname($lua), cfg => $cfg);

diag $sh1->log unless
ok $sh1->started, 'Shard1 is started ' . $sh1->primary_port;

my $sh2 =
    run DR::Tarantool::StartTest(script_dir => dirname($lua), cfg => $cfg);

diag $sh2->log unless
    ok $sh2->started, 'Shard1 is started ' . $sh2->primary_port;


my $spaces = {
    0 => {
        name => 'test',
        fields  => [ { type => 'STR', name => 'id' }, qw(f1 f2) ],
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


note '* sharding *';
{

    is $t1->call_lua('box.shard.schema.is_valid' => [])->raw(0), 0,
        'schema1 is not valid';
    is $t2->call_lua('box.shard.schema.is_valid' => [])->raw(0), 0,
        'schema2 is not valid';

    load_func $t1, curr => q{
        function(space, key)
            print('curr(' .. tostring(box.tuple.new({ space, key })) .. ')')
            return 1 + math.fmod(string.byte(key, 1), 2)
        end
    };
    load_func $t2, curr => q{
        function(space, key)
            print('curr(' .. tostring(box.tuple.new({ space, key })) .. ')')
            return 1 + math.fmod(string.byte(key, 1), 2)
        end
    };

    diag $sh1->log unless
    config $t1,
        list => qq#
            {
                { { 'rw', 100, '127.0.0.1', @{[$sh1->primary_port]} }, },
                { { 'rw', 100, '127.0.0.1', @{[$sh2->primary_port]} }, },
            }
        #,
        me  => 2,
        mode    => "'rw'",
    ;
    diag $sh2->log unless
    config $t2,
        list => qq#
            {
                { { 'rw', 100, '127.0.0.1', @{[$sh1->primary_port]} }, },
                { { 'rw', 100, '127.0.0.1', @{[$sh2->primary_port]} }, },
            }
        #,
        me  => 2,
        mode    => "'rw'",
    ;

    is $t1->call_lua('box.shard.schema.is_valid' => [])->raw(0), 1,
        'schema1 is valid';
    is $t2->call_lua('box.shard.schema.is_valid' => [])->raw(0), 1,
        'schema2 is valid';

    eval { $t1->call_lua('box.shard.insert', [ '0', 'b', 'c', 'd']) };
    like $@, qr{Can't redirect}, "ttl";

    config $t1, me => 1;

    is_deeply $t1->call_lua('box.shard.insert', [ '0', 'b', 'c', 'd'])->raw,
        ['b', 'c', 'd'],
        'box.shard.insert(0, "b", "c", "d")';

    is_deeply $t1->call_lua('box.shard.insert', [ '0', 'a', 'b', 'c'])->raw,
        ['a', 'b', 'c'],
        'box.shard.insert(0, "a", "b", "c")';


    is_deeply $t1->select(test => ['b'])->raw, ['b', 'c', 'd'],
        'first record is in the first shard';
    is_deeply $t2->select(test => ['a'])->raw, ['a', 'b', 'c'],
        'second record is in the second shard';


    is_deeply
        $t1->call_lua('box.shard.replace', [ '0', 'b', 'c', 'd', 'e'])->raw,
        ['b', 'c', 'd', 'e'],
        'box.shard.replace(0, "b", "c", "d", "e")';

    is_deeply
        $t1->call_lua('box.shard.replace', [ '0', 'a', 'b', 'c', 'd'])->raw,
        ['a', 'b', 'c', 'd'],
        'box.shard.replace(0, "a", "b", "c", "d")';


    is_deeply $t1->select(test => ['b'])->raw, ['b', 'c', 'd', 'e'],
        'first record is in the first shard';
    is_deeply $t2->select(test => ['a'])->raw, ['a', 'b', 'c', 'd'],
        'second record is in the second shard';


    is_deeply $t1->call_lua('box.shard.select', [ 0, 'b' ])->raw,
        ['b', 'c', 'd', 'e'],
        "box.shard.select'(0, 'b')";
    is_deeply $t1->call_lua('box.shard.select', [ 0, 'a' ])->raw,
        ['a', 'b', 'c', 'd'],
        "box.shard.select'(0, 'a')";

    is_deeply $t1->call_lua('box.shard.delete', [ 0, 'b' ])->raw,
        ['b', 'c', 'd', 'e'],
        'box.shard.delete(0, "b")';
    is_deeply $t1->call_lua('box.shard.delete', [ 0, 'b' ]), undef,
        'box.shard.delete(0, "b") - repeat';


    is_deeply $t1->call_lua('box.shard.delete', [ 0, 'a' ])->raw,
        ['a', 'b', 'c', 'd'],
        'box.shard.delete(0, "a")';
    is_deeply $t1->call_lua('box.shard.delete', [ 0, 'a' ]), undef,
        'box.shard.delete(0, "a") - repeat';



    is_deeply $t1->call_lua('box.shard.insert', [ 0, 'a', 'b' ])->raw,
        ['a', 'b'],
        'box.shard.insert(a, b)';

    is_deeply $t1->call_lua('box.shard.insert', [ 0, 'b', 'c' ])->raw,
        ['b', 'c'],
        'box.shard.insert(b, c)';


    is_deeply $t1->call_lua('box.shard.update', [ 0, 'a', '=p', 1, 'c' ])->raw,
        ['a', 'c'],
        'box.shard.update(0, a, =p, 1, c)';

    is_deeply $t1->call_lua('box.shard.update', [ 0, 'b', '=p', 1, 'd' ])->raw,
        ['b', 'd'],
        'box.shard.update(0, b, =p, 1, d)';

    is_deeply $t1->select(test => ['b'])->raw, ['b', 'd'],
        'first record is in the first shard';
    is_deeply $t2->select(test => ['a'])->raw, ['a', 'c'],
        'second record is in the second shard';

    is_deeply $t2->call_lua('box.shard.delete', [ 0, 'a' ])->raw,
        ['a',  'c'],
        'cleanup shard';
    is_deeply $t2->call_lua('box.shard.delete', [ 0, 'b' ])->raw,
        ['b',  'd'],
        'cleanup shard';

    is
        $t2->call_lua('box.dostring', [ 'return tostring(box.space[0]:len())'])
            ->raw(0), 0, 'space 0 (shard2) is empty';
    is
        $t1->call_lua('box.dostring', [ 'return tostring(box.space[0]:len())'])
            ->raw(0), 0, 'space 0 (shard1) is empty';
}
note '* resharding *';
{
    is_deeply $t1->insert(test => [ 'a', 'b' ], TNT_FLAG_RETURN)->raw,
        ['a', 'b'],
        'prev schema insert sh1';

    is_deeply $t2->insert(test => [ 'b', 'c' ], TNT_FLAG_RETURN)->raw,
        ['b', 'c'],
        'prev schema insert sh1';

    is_deeply $t1->call_lua('box.shard.select', [ 0, 'a' ]), undef,
        'no records in storage';

    is_deeply $t2->call_lua('box.shard.select', [ 0, 'a' ]), undef,
        'no records in storage';

    is_deeply $t1->call_lua('box.shard.select', [ 0, 'b' ]), undef,
        'no records in storage';

    is_deeply $t2->call_lua('box.shard.select', [ 0, 'b' ]), undef,
        'no records in storage';

    load_func $t1, prev => q{
        function(space, key)
            print('curr(' .. tostring(box.tuple.new({ space, key })) .. ')')
            return 1 + 1 - math.fmod(string.byte(key, 1), 2)
        end
    };
    load_func $t2, prev => q{
        function(space, key)
            print('curr(' .. tostring(box.tuple.new({ space, key })) .. ')')
            return 1 + 1 - math.fmod(string.byte(key, 1), 2)
        end
    };
    is $t1->call_lua('box.shard.schema.is_valid' => [])->raw(0), 1,
        'schema1 is valid';
    is $t2->call_lua('box.shard.schema.is_valid' => [])->raw(0), 1,
        'schema2 is valid';


    is_deeply $t1->call_lua('box.shard.select', [ 0, 'a' ])->raw, ['a', 'b'],
        'record 1 by `prev` schema';

    is_deeply $t2->call_lua('box.shard.select', [ 0, 'b' ])->raw, ['b', 'c'],
        'record 2 by `prev` schema';


    note ' insert collision';
    is eval {
        $t1->call_lua('box.shard.insert', [0, 'a', 'c']);
        1;
    }, undef, 'insert error';
    like $@, qr{Duplicate key exists}, 'error message';
    is eval {
        $t1->call_lua('box.shard.insert', [0, 'b', 'd']);
        1;
    }, undef, 'insert error';
    like $@, qr{Duplicate key exists}, 'error message';
  
    note ' replace';
    is_deeply
        $t1->call_lua('box.shard.replace', [0, 'a', 'aa'])->raw,
        [ 'a', 'aa' ],
        'replace first record';
    is_deeply
        $t1->call_lua('box.shard.replace', [0, 'b', 'bb'])->raw,
        [ 'b', 'bb' ],
        'replace second record';


    is_deeply
        $t1->call_lua('box.shard.select', [ 0, 'a' ])->raw,
        [ 'a', 'aa' ],
        'select data from shards';
    is_deeply
        $t1->call_lua('box.shard.select', [ 0, 'b' ])->raw,
        [ 'b', 'bb' ],
        'select data from shards';
    
    is_deeply $t1->select(test => [ 'a' ])->raw,
        ['a', 'b'],
        'prev data is not changed (sh1)';

    is_deeply $t2->select(test => [ 'b' ])->raw,
        ['b', 'c'],
        'prev data is not changed (sh2)';

    $t2->delete(test => 'a');
    $t1->delete(test => 'b');
    
    is_deeply
        $t1->call_lua('box.shard.select', [ 0, 'a' ])->raw,
        [ 'a', 'b' ],
        'back to prev state';
    is_deeply
        $t1->call_lua('box.shard.select', [ 0, 'b' ])->raw,
        [ 'b', 'c' ],
        'back to prev state';


    note ' update';

    is_deeply
        $t1->call_lua('box.shard.update', [ 0 => 'a', '=p', 1, 'aaa' ])->raw,
        [ 'a', 'aaa' ],
        'update the first record';

    is_deeply
        $t1->call_lua('box.shard.update', [ 0 => 'b', '=p', 1, 'bbb' ])->raw,
        [ 'b', 'bbb' ],
        'update the second record';
    is_deeply $t1->select(test => [ 'a' ])->raw,
        ['a', 'b'],
        'prev data is not changed (sh1)';

    is_deeply $t2->select(test => [ 'b' ])->raw,
        ['b', 'c'],
        'prev data is not changed (sh2)';
    is_deeply
        $t1->call_lua('box.shard.select', [ 0, 'a' ])->raw,
        [ 'a', 'aaa' ],
        'select the first record';
    is_deeply
        $t1->call_lua('box.shard.select', [ 0, 'b' ])->raw,
        [ 'b', 'bbb' ],
        'select the second record';
}
