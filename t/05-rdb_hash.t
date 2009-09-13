use strict;
use warnings;
use Test::More tests => 17;
use Test::ttserver;

BEGIN { use_ok 'Engage::DOD::TokyoTyrant' }
$ENV{'CONFIG_PATH'} = 't/conf';

my $dod = new_ok( 'Engage::DOD::TokyoTyrant' );

my @ttservers;
for my $socket (@{ $dod->sockets('hash') }) {
    my $ttserver = Test::ttserver->new('hash.tch',
        port => $socket->{'port'},
    ) or plan 'skip_all' => $Test::ttserver::errstr;
    push @ttservers, $ttserver;
}

my $storage = $dod->storage('hash');

#=============================================================================
# CREATE & READ
#=============================================================================
is( $storage->read('foo'), undef, 'key is not exists yet' );
ok( $storage->create('foo' => 1), 'create single key' );
ok(!$storage->create('foo' => 1), 'create duplicate single key' );
is( $storage->read('foo'), 1, 'read single key' );
ok( $storage->create( a => 1, b => 2, c => 3 ), 'create multiple keys' ); 
ok(!$storage->create( a => 1, b => 2, c => 3 ), 'create duplicate multiple keys' ); 
is_deeply( $storage->read(qw/a b c/), { a => 1, b => 2, c => 3 }, 'read multiple keys' );

#=============================================================================
# UPDATE
#=============================================================================
ok(!$storage->update( z => 99 ), 'no keys to update' );
ok( $storage->update( a => 9 ), 'update single key' );
is( $storage->read('a'), 9, 'updated single key' );
ok( $storage->update( a => 4, b => 5, c => 6 ), 'update keys success' );
is_deeply( $storage->read(qw(a b c)),
    { a => 4, b => 5, c => 6 },
'update and read multiple keys' );

#=============================================================================
# DELETE
#=============================================================================
ok( $storage->delete('a'), 'delete single key' );
ok( $storage->delete(qw(b c)), 'delete multiple keys' );
is( $storage->read(qw(a b c)), undef, 'deleted multiple keys' );

