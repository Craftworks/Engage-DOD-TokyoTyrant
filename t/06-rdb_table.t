use strict;
use warnings;
use Test::More tests => 19;
use Test::ttserver;

BEGIN { use_ok 'Engage::DOD::TokyoTyrant' }
$ENV{'CONFIG_PATH'} = 't/conf';

my $dod = new_ok( 'Engage::DOD::TokyoTyrant' );

my @ttservers;
for my $socket (@{ $dod->sockets('table') }) {
    my $ttserver = Test::ttserver->new('table.tct',
        port => $socket->{'port'},
    ) or plan 'skip_all' => $Test::ttserver::errstr;
    push @ttservers, $ttserver;
}

my $storage = $dod->storage('table');

#=============================================================================
# CREATE & READ
#=============================================================================
is( $storage->read('bob'), undef, 'key is not exists yet' );
ok( $storage->create('bob', +{ blood => 'A', age => 20 }), 'create key success' );
ok(!$storage->create('bob', +{ blood => 'A', age => 20 }), 'create key failed' );
is_deeply( $storage->read('bob'), +{ blood => 'A', age => 20 }, 'read key success' );
ok( $storage->create(
    'michael' => { blood => 'B', age => 22 },
    'janet'   => { blood => 'O', age => 18 },
    'chris'   => { blood => 'A', age => 26 },
), 'create multiple keys success' );
ok(!$storage->create(
    'michael' => { blood => 'B', age => 22 },
    'janet'   => { blood => 'O', age => 18 },
    'chris'   => { blood => 'A', age => 26 },
), 'create multiple keys failed' );
is_deeply( $storage->read(qw/michael janet chris/), {
    'michael' => { blood => 'B', age => 22 },
    'janet'   => { blood => 'O', age => 18 },
    'chris'   => { blood => 'A', age => 26 },
}, 'read multiple keys success' );

#=============================================================================
# Search
#=============================================================================
is_deeply( $storage->read({
    age => { '>' => 20 },
}, {
    order => { 'age' => 'NUM_DESC' },
}), [
    { 'chris'   => { blood => 'A', age => 26 } },
    { 'michael' => { blood => 'B', age => 22 } },
], 'search by cond' );
is_deeply( $storage->read(undef, {
    order => { 'blood' => 'STR_DESC' },
}), [
    { 'janet'   => { blood => 'O', age => 18 } },
    { 'michael' => { blood => 'B', age => 22 } },
    { 'bob'     => { blood => 'A', age => 20 } },
    { 'chris'   => { blood => 'A', age => 26 } },
], 'search all order desc' );

#=============================================================================
# UPDATE
#=============================================================================
ok( $storage->update( 'bob' => { blood => 'A', age => 21 } ), 'update key success' );
is_deeply( $storage->read('bob'), { blood => 'A', age => 21 }, 'update and read key success' );
ok( $storage->update(
    'michael' => { blood => 'B', age => 32 },
    'janet'   => { blood => 'O', age => 28 },
), 'update multiple keys success' );
is_deeply( $storage->read(qw/michael janet/), {
    'michael' => { blood => 'B', age => 32 },
    'janet'   => { blood => 'O', age => 28 },
}, 'update and read multiple keys success' );

#=============================================================================
# DELETE
#=============================================================================
ok( $storage->delete('bob'), 'delete single key success' );
ok(!$storage->read('bob'), 'delete and read single key success' );
ok( $storage->delete(qw/michael janet chris/), 'delete multiple keys' );
ok(!$storage->read(qw/michael janet chris/), 'delete and read multiple keys' );

