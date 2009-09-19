use strict;
use warnings;
use Test::More tests => 18;
use Test::ttserver;

BEGIN { use_ok 'Engage::DOD::TokyoTyrant' }
$ENV{'CONFIG_PATH'} = 't/conf';

my $dod = new_ok( 'Engage::DOD::TokyoTyrant' );

my @ttservers;
for my $socket (@{ $dod->sockets }) {
    my $ttserver = Test::ttserver->new(undef,
        port => $socket->{'port'},
    ) or plan 'skip_all' => $Test::ttserver::errstr;
    push @ttservers, $ttserver;
}

#=============================================================================
# rdb
#=============================================================================
isa_ok( $dod->storage('hash')->rdb('W'), 'TokyoTyrant::RDB' );
isa_ok( $dod->storage('hash')->rdb('R'), 'TokyoTyrant::RDB' );
isa_ok( $dod->storage('table')->rdb('W'), 'TokyoTyrant::RDBTBL' );
isa_ok( $dod->storage('table')->rdb('R'), 'TokyoTyrant::RDBTBL' );

#=============================================================================
# reconnect
#=============================================================================
$dod->storage('table')->rdb('R');
ok( $dod->storage('table')->rdb('R')->close, 'close connection' );
ok( $dod->storage('table')->rdb('R')->stat, 'auto reconnect' );

#=============================================================================
# _connect_info
#=============================================================================
is( $dod->storage('hash')->_connect_info('W'),  'localhost:101980', 'connect info hash W');
is( $dod->storage('table')->_connect_info('W'), 'localhost:101981', 'connect info table W');
is( $dod->storage('table')->_connect_info('R'), 'localhost:101981', 'connect info table R');
is( $dod->storage('multi')->_connect_info('W'), 'localhost:101971', 'connect info multi W');
is( $dod->storage('dual')->_connect_info('W'),  'localhost:101978', 'connect info dual W');
is( $dod->storage('dual')->_connect_info('R'),  'localhost:101979', 'connect info dual R');

my $storage = $dod->storage('multi');
my %counter;
$counter{ $storage->_connect_info('R') }++ for (1 .. 100000);
for my $connect_info (sort keys %counter) {
    my $ratio = $counter{$connect_info} / 1000;
    ok( 24 < $ratio && $ratio < 26, "balance of $connect_info" );
}

