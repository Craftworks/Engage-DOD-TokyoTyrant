package Engage::DAO::Role::TokyoTyrant;

use Moose::Role;
no Moose::Role;

sub storage {
    my $self = shift;
    $self->dod('TokyoTyrant')->storage( $self->data_name );
}

sub create { shift->storage->create( @_ ); }
sub read   { shift->storage->read  ( @_ ); }
sub update { shift->storage->update( @_ ); }
sub delete { shift->storage->delete( @_ ); }
sub update_or_create { shift->storage->update_or_create( @_ ); }

1;
