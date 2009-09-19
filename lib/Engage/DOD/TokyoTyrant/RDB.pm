package Engage::DOD::TokyoTyrant::RDB;

use Moose;
extends 'Engage::DOD::TokyoTyrant::Base';
with 'Engage::DOD::Role::Driver';

has '+connections' => (
    isa => 'HashRef[TokyoTyrant::RDB]',
);

no Moose;

__PACKAGE__->meta->make_immutable;

sub storage_class { 'TokyoTyrant::RDB' }

sub create {
    my ( $self, @rows ) = @_;

    my $rdb = $self->rdb('W');
    my $has_error = 0;

    while ( my ($key, $val) = splice @rows, 0, 2 ) {
        unless ( $rdb->putkeep($key, $val) ) {
            my $msg = $rdb->errmsg($rdb->ecode);
            $self->log->error(qq/create failed: $msg "$key"/);
            $has_error = 1;
        }
    }

    return !$has_error;
}

sub read {
    my $self = shift;

    my $rdb = $self->rdb('R');

    # single
    if ( @_ == 1 ) {
        return $rdb->get(shift);
    }
    # multiple
    else {
        my $recs = +{ map { $_, undef } @_ };
        $rdb->mget($recs);
        return %$recs ? $recs : undef;
    }
}

sub update {
    my $self = shift;

    unless (@_ && !grep ref, @_) {
        Engage::Exception->throw('Specify the keys to update');
    }

    my $rdb = $self->rdb('W');
    my @rows;
    my $has_error = 0;

    @rows = @_;
    while ( my ($key) = splice @rows, 0, 2 ) {
        unless ( defined $rdb->get($key) ) {
            my $msg = $rdb->errmsg($rdb->ecode);
            $self->log->error(qq/update failed: $msg "$key"/);
            $has_error = 1;
        }
    }
    return 0 if $has_error;

    @rows = @_;
    while ( my ($key, $val) = splice @rows, 0, 2 ) {
        unless ( defined $rdb->put($key, $val) ) {
            my $msg = $rdb->errmsg($rdb->ecode);
            $self->log->error(qq/update failed: $msg "$key"/);
            $has_error = 1;
        }
    }

    return !$has_error;
}

sub delete {
    my $self = shift;

    my $rdb = $self->rdb('W');
    my $has_error = 0;

    for my $key ( @_ ) {
        unless ( $rdb->out($key) ) {
            my $msg = $rdb->errmsg($rdb->ecode);
            $self->log->error(qq/delete failed: $msg "$key"/);
            $has_error = 1;
        }
    }

    return !$has_error;
}

1;
