package Engage::DOD::TokyoTyrant;

use Moose;
use MooseX::AttributeHelpers;
use TokyoTyrant;

extends 'Engage::DOD';

our $VERSION = '0.001';

has 'storage_class' => (
    is  => 'ro',
    isa => 'HashRef[Str]',
    default   => sub { {} },
    metaclass => 'Collection::Hash',
    provides  => {
        'set' => 'set_storage_class',
        'get' => 'get_storage_class',
    },
);

has 'storages' => (
    is  => 'ro',
    isa => 'HashRef',
    default => sub { {} },
    lazy => 1,
);

no Moose;

__PACKAGE__->meta->make_immutable;

sub BUILD {
    my $self = shift;

    for my $storage (qw/RDB RDBTBL/) {
        my $class;
        my $base = __PACKAGE__ . "\::$storage";
        my $orig = ref($self)  . "\::$storage";

        local $@;
        if ( eval "require $orig" ) {
            $class = $orig;
        }
        elsif ( eval "require $base" ) {
            $class = $base;
        }
        else {
            confess $@;
        }

        Class::MOP::load_class( $class );
        $self->set_storage_class($storage => $class);
    }
}

sub storage {
    my ( $self, $database ) = @_;

    confess q{Usage: $dod->('DBI')->storage('Database')}
        unless defined $database;

    my $config = $self->config->{'databases'}{$database};
    confess qq{Unknown database "$database"} unless defined $config;

    my $storage_class = $self->get_storage_class( $config->{'storage_class'} );
    confess qq{Unknown storage "$storage_class"}
        unless Class::MOP::is_class_loaded( $storage_class );

    unless ( $self->storages->{$database} ) {
        $self->log->info(q/The storage is not instantiated yet. Trying to instantiate./);
        $self->storages->{$database} = $storage_class->new( $config );
    }

    return $self->storages->{$database};
}

sub sockets {
    my ( $self, $database ) = @_;

    my @sockets;
    for my $name (keys %{ $self->config->{'databases'} }) {
        next if ( defined $database && $name ne $database );
        my $db = $self->config->{'databases'}{$name};
        my $sources = $db->{'datasources'};
        push @sockets, (ref $_ eq 'HASH') ? $_ : @$_ for values %$sources;
    }

    return \@sockets;
}

1;

=head1 NAME

Engage::DOD::TokyoTyrant - The great new Engage::DOD::TokyoTyrant!

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Engage::DOD::TokyoTyrant;

    my $foo = Engage::DOD::TokyoTyrant->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 METHODS

=head1 AUTHOR

Craftworks, C<< <craftwork at cpan org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-engage-dod-tokyotyrant at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Engage-DOD-TokyoTyrant>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Engage::DOD::TokyoTyrant

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Engage-DOD-TokyoTyrant>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Engage-DOD-TokyoTyrant>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Engage-DOD-TokyoTyrant>

=item * Search CPAN

L<http://search.cpan.org/dist/Engage-DOD-TokyoTyrant/>

=back

=head1 ACKNOWLEDGEMENTS

=head1 COPYRIGHT & LICENSE

Copyright 2009 Craftworks.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
