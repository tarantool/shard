#!/usr/bin/perl

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);

use Test::More tests    => 54;
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

for my $c ($t1, $t2) {
    state $no = 0;
    my $done = $c->call_lua('box.dostring', [
        qq/
            local space = box.schema.create_space('test', {
                id = 0,
            })

            space:create_index('primary', 'tree', {
                parts = { 0, 'num' }
            })

            return '1'
        /
    ]);
    $no++;
    is $done->raw(0), 1, 'space[0] created, shard ' . $no;


    for (1 .. 100) {
        $c->insert(test => [ $_, rand, rand ])
            if ($no % 2 xor $_ % 2);
    }
    
    $done = $c->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ]);
    is $done->raw(0), 50, '50 records were inserted at shard ' . $no;
}
for my $c ($t1, $t2) {
    $c->call_lua('box.dostring', [
        qq[
            box.shard.schema.config({key = function(space, key)
                if type(key) ~= 'number' then
                    key = box.unpack('i', key)
                end
                if key % 2 == 1 then
                    return 2
                else
                    return 1
                end
            end})
        ]
    ]);
}

note 'select';
{
    my (@r1, @r2);
    for ( 1  .. 100 ) {
        my $t = $t1->call_lua('box.dostring', [
            qq/
                return box.shard.select(
                    'test',
                    $_
                )
            /
        ]);
        push @r1 => $t if defined $t;
        
        $t = $t2->call_lua('box.dostring', [
            qq/
                return box.shard.select(
                    'test',
                    $_
                )
            /
        ]);
        push @r2 => $t if defined $t;
    }
    is scalar @r1, 100, '100 results from the first shard';
    is scalar @r2, 100, '100 results from the second shard';
}

note 'insert';
{
    for (101 .. 190) {
        $t1->call_lua('box.dostring', [
                qq{
                    return box.shard.insert('test', $_, '2', '3')
                }
            ]
        );
    }
    is $t1->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ])->raw(0), 95,
            '95 records in the first shard';
    is $t2->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ])->raw(0), 95,
        '95 records in the second shard';
}

note 'update';
{
    for(1 .. 190) {
        $t1->call_lua('box.dostring', [
                qq{
                    return box.shard.update('test', $_, '=p', 1, '15121974')
                }
            ]
        );
    }
    my ($c1, $c2, $c3) = (0, 0, 0);
    for (1 .. 190) {
        if ( $_ % 2 ) {
            $c2++ if $t2->select(test => [ $_ ])->raw(1) eq '15121974';
        } else {
            $c3++ if $t1->select(test => [ $_ ])->raw(1) eq '15121974';
        }
        $c1++ unless defined $t1->select(test => [ $_ ]);
    }
    is $c1, 95, '95 records are in the first shard';
    is $c2, 95, '95 records were modified';
    is $c3, 95, '95 records were modified';
}

note 'delete';
{
    for (11 .. 190) {
        $t1->call_lua('box.dostring', [
                qq{
                    return box.shard.delete('test', $_)
                }
            ]
        );
    }
    is $t1->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ])->raw(0), 5,
            '90 records were deleted in the first shard';
    is $t2->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ])->raw(0), 5,
        '90 records were deleted in the second shard';
}

for my $c ($t1, $t2) {
    $c->call_lua('box.dostring', [
        qq[
            box.shard.schema.config({
                old_key = box.shard.schema.key,
                key     = function(space, ...) return 1 end
            })
        ]
    ]);
}

note '================ resharding ===================';
note 'move';
{
    my @res;
    for (1 .. 10) {
        push @res => $t2->call_lua('box.dostring', [
            qq{ return box.shard.select('test', $_) }
        ], 'test');
    }
    is scalar @res, 10, '10 records were fetched';
    is scalar grep({ defined $_ } @res), 10, '10 records were fetched really';

    is $t1->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ])->raw(0), 10,
            'All records were migrated to the first shard';
    is $t2->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ])->raw(0), 0,
            'All records were migrated to the first shard';
   
    @res = ();
    for (1001 .. 1010) {
        my $c = ($_ % 2) ? $t1 : $t2;
        push @res => $c->call_lua('box.dostring', [
            qq{ return box.shard.select( 'test', $_ ) }
        ], 'test');
    }
    is_deeply \@res, [ map { undef } 1 .. 10 ], 'select absent records';
}

note 'delete';
{
    my @res;
    for (1 .. 10) {
        my $c = ($_ % 2) ? $t1 : $t2;
        push @res => $c->call_lua('box.dostring', [
            qq{ return box.shard.delete( 'test', $_ ) }
        ], 'test');
    }
    is scalar @res, 10, '10 records were deleted';
    is scalar grep({ defined $_ } @res), 10, '10 records were deleted really';
    
    is $t1->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ])->raw(0), 0,
            'All records were deleted';
    is $t2->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ])->raw(0), 0,
            'All records were deleted';

    @res = ();
    for (1 .. 10) {
        my $c = ($_ % 2) ? $t1 : $t2;
        push @res => $c->call_lua('box.dostring', [
            qq{ return box.shard.delete( 'test', $_ ) }
        ], 'test');
    }
    is_deeply \@res, [ map { undef } 1 .. 10 ], 'delete absent records';
}

note 'insert';
{
    my @res;
    for (1 .. 10) {
        my $c = ($_ % 2) ? $t1 : $t2;
        push @res => $c->call_lua('box.dostring', [
            qq{ return box.shard.insert( 'test', $_, 'cde' ) }
        ], 'test');
    }
    is scalar @res, 10, '10 records were deleted';
    is scalar grep({ defined $_ } @res), 10, '10 records were inserted really';
    
    is $t1->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ])->raw(0), 10,
            'All records were inserted into the first shard';
    is $t2->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ])->raw(0), 0,
            'No one records were inserted into the second shard';
}


note 'update';
{
    my @res;
    for (1 .. 10) {
        my $c = ($_ % 2) ? $t1 : $t2;
        push @res => $c->call_lua('box.dostring', [
            qq{ return box.shard.update( 'test', $_, '=p', 1, 'abcd' ) }
        ], 'test');
    }
    is scalar @res, 10, '10 records were updated';
    is scalar grep({ defined $_ and $_->f1 eq 'abcd' } @res), 10,
        '10 records were updated really';
    
    for my $c ($t1, $t2) {
        $c->call_lua('box.dostring', [
            qq[
                box.shard.schema.config({
                    old_key = box.shard.schema.key,
                    key     = function(space, ...) return 2 end
                })
            ]
        ]);
    }
   
    @res = ();
    for (1 .. 10) {
        my $c = ($_ % 2) ? $t1 : $t2;
        push @res => $c->call_lua('box.dostring', [
            qq{ return box.shard.update( 'test', $_, '=p', 1, 'abcde' ) }
        ], 'test');
    }
    is scalar @res, 10, '10 records were updated';
    is scalar grep({ defined $_ and $_->f1 eq 'abcde' } @res), 10,
        '10 records were updated really';
    
    is $t1->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ])->raw(0), 0,
            'All records were inserted into the second shard';
    is $t2->call_lua('box.dostring',
        [ 'return tostring(box.space[0]:len())' ])->raw(0), 10,
            'No one records were migrated into the second shard';
}

{
    for my $c ($t1, $t2) {
        $c->call_lua('box.dostring', [
            qq[
                box.shard.schema.config({
                    old_key             = box.shard.schema.key,
                    key                 = function(space, ...) return 1 end,
                    daemon              = true,
                    daemon_off_period   = 0.5,
                })
            ]
        ]);
    }

    my $started = AnyEvent::now;
    for ( 1 .. 20 ) {
        Coro::AnyEvent::sleep .1;

        my $d = $t2->call_lua('box.dostring',
            [ 'return type(box.shard.daemon)' ])->raw(0);
        last if $d eq 'nil';

    }

    my $time = AnyEvent::now() - $started;

    cmp_ok $time, '>=', .5, 'daemon was delayed to .5 seconds';
    diag $sh1->log unless
        is $t1->call_lua('box.dostring',
            [ 'return tostring(box.space[0]:len())' ])->raw(0), 10,
                'All records were inserted into the second shard';


    diag $sh2->log unless
        is $t2->call_lua('box.dostring',
            [ 'return tostring(box.space[0]:len())' ])->raw(0), 0,
                'No one records were migrated into the second shard';
}

END {
# note '=========================================';
# note $sh1->log;
# note '=========================================';
# note $sh2->log;
# note '=========================================';
}
