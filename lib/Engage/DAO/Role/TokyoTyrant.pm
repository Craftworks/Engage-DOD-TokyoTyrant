package Engage::DAO::Role::TokyoTyrant;

use Moose::Role;
no Moose::Role;

sub storage {
    my ( $self, $datasource ) = @_;
    $self->dod('TokyoTyrant')->storage( $self->data_name, $datasource );
}

sub create { shift->storage('W')->create( @_ ); } 
sub read   { shift->storage('R')->read  ( @_ ); } 
sub update { shift->storage('W')->update( @_ ); } 
sub delete { shift->storage('W')->delete( @_ ); } 

1;
