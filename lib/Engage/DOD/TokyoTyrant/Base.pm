package Engage::DOD::TokyoTyrant::Base;

use Moose;
extends 'Engage::DOD';

has 'replication' => (
    is  => 'ro',
    isa => 'Str',
    default => 'multislave',
);

has 'datasources' => (
    is  => 'ro',
    isa => 'HashRef',
    required => 1,
);

has 'connections' => (
    is  => 'ro',
    isa => 'HashRef[Object]',
    default => sub { {} },
    metaclass => 'Collection::Hash',
    provides => {
        'get' => 'get_connection',
        'set' => 'set_connection',
    },
    lazy => 1,
);

no Moose;

__PACKAGE__->meta->make_immutable;

sub rdb {
    my ( $self, $datasource ) = @_;

    my $connect_info = $self->_connect_info( $datasource );
    my $rdb = $self->get_connection($connect_info);

    # first time
    unless ( defined $rdb ) {
        $self->log->info(q/The storage is not connected yet.  Trying to connect./);
        $rdb = $self->set_connection( $connect_info => $self->_connect($connect_info) );
    }
    # auto reconnect
    unless ( $rdb->stat ) {
        $self->log->info(q/The connection is not available.  Trying to reconnect./);
        $rdb = $self->set_connection( $connect_info => $self->_connect($connect_info) );
    }

    return $rdb;
}

sub _connect_info {
    my ( $self, $datasource ) = @_;

    $datasource = 'RW' if ( $self->replication eq 'no' );
    my $connect_info;

    if ( $self->replication eq 'multislave' && $datasource eq 'R' ) {
        my $index = int rand @{ $self->datasources->{$datasource} };
        $connect_info = $self->datasources->{$datasource}[$index];
    }
    else {
        $connect_info = $self->datasources->{$datasource};
    }

    confess qq{Unknown datasource "$datasource"} unless defined $connect_info;
    confess qq{Specify host and port "$datasource"}
        unless defined $connect_info->{'host'} and length $connect_info->{'port'};

    return sprintf '%s:%s', @$connect_info{qw/host port/};
}

sub _connect {
    my ( $self, $connect_info ) = @_;
    my $rdb = $self->storage_class->new;
    unless ( $rdb->open(split ':', $connect_info) ) {
        Engage::Exception->throw(sprintf '%s %s', $rdb->errmsg($rdb->ecode), $connect_info)
    }
    return $rdb;
}

1;
