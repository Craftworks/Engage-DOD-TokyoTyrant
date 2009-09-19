package Engage::DOD::TokyoTyrant::RDBTBL;

use Moose;
extends 'Engage::DOD::TokyoTyrant::Base';
with 'Engage::DOD::Role::Driver';

has '+connections' => (
    isa => 'HashRef[TokyoTyrant::RDBTBL]',
);

no Moose;

__PACKAGE__->meta->make_immutable;

our %QueryConditon = (
    'QCSTREQ'   => 'QCSTREQ',   # string which is equal to the expression
    'QCSTRINC'  => 'QCSTRINC',  # string which is included in the expression
    'QCSTRBW'   => 'QCSTRBW',   # string which begins with the expression
    'QCSTREW'   => 'QCSTREW',   # string which ends with the expression
    'QCSTRAND'  => 'QCSTRAND',  # string which includes all tokens in the expression
    'QCSTROR'   => 'QCSTROR',   # string which includes at least one token in the expression
    'QCSTROREQ' => 'QCSTROREQ', # string which is equal to at least one token in the expression
    'QCSTRRX'   => 'QCSTRRX',   # string which matches regular expressions of the expression
    'QCNUMEQ'   => 'QCNUMEQ',   # number which is equal to the expression
    'QCNUMGT'   => 'QCNUMGT',   # number which is greater than the expression
    'QCNUMGE'   => 'QCNUMGE',   # number which is greater than or equal to the expression
    'QCNUMLT'   => 'QCNUMLT',   # number which is less than the expression
    'QCNUMLE'   => 'QCNUMLE',   # number which is less than or equal to the expression
    'QCNUMBT'   => 'QCNUMBT',   # number which is between two tokens of the expression
    'QCNUMOREQ' => 'QCNUMOREQ', # number which is equal to at least one token in the expression
    'QCFTSPH'   => 'QCFTSPH',   # full-text search with the phrase of the expression
    'QCFTSAND'  => 'QCFTSAND',  # full-text search with all tokens in the expression
    'QCFTSOR'   => 'QCFTSOR',   # full-text search with at least one token in the expression
    'QCFTSEX'   => 'QCFTSEX',   # full-text search with the compound expression.
                                # All operations can be flagged by bitwise-or
    'QCNEGATE'  => 'QCNEGATE',  # negation
    'QCNOIDX'   => 'QCNOIDX',   # using no index
    'eq'        => 'QCSTREQ',
    'and'       => 'QCSTRAND',
    'or'        => 'QCSTROR',
    'like'      => 'QCSTRINC',
    'like_begin'=> 'QCSTRBW',
    'like_end'  => 'QCSTREW',
    'regex'     => 'QCSTRRX',
    '=='        => 'QCNUMEQ',
    '>'         => 'QCNUMGT',
    '>='        => 'QCNUMGE',
    '<'         => 'QCNUMLT',
    '<='        => 'QCNUMLE',
    'between'   => 'QCNUMBT',
);

sub storage_class { 'TokyoTyrant::RDBTBL' }

sub create {
    my ( $self, @rows ) = @_;

    my $rdb = $self->rdb('W');
    my $has_error = 0;

    while ( my ($key, $cols) = splice @rows, 0, 2 ) {
        unless ( ref $cols eq 'HASH' ) {
            Engage::Exception->throw('Columns must be hash reference');
        }
        unless ( $rdb->putkeep($key, $cols) ) {
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

    # has query conditions
    if ( grep ref, @_ ) {
        return $self->search(@_);
    }
    # single
    elsif ( @_ == 1 ) {
        my $key  = shift;
        return $rdb->get($key);
    }
    # multiple
    elsif ( @_ != 1 ) {
        my $recs = +{ map { $_, undef } @_ };
        $rdb->mget($recs);
        return %$recs ? $recs : undef;
    }
}

sub search {
    my ( $self, $where, $opt, $attr ) = @_;

    my $rdb = $self->rdb('R');
    my $qry = TokyoTyrant::RDBQRY->new( $rdb );

    while (my ($name, $cond) = each %$where ) {
        my ( $op, $expr ) = each %$cond;
        my $query_condition = $QueryConditon{$op};
        $expr = join q{,}, @$expr if ref $expr eq 'ARRAY';
        $qry->addcond( $name, $qry->$query_condition, $expr );
    }

    if ( my $order = $opt->{'order'} ) {
        my ( $name, $type ) = each %$order;
        $type = uc 'QO' . $type;
        $type =~ tr/_//d;
        $qry->setorder( $name, $qry->$type );
    }

    if ( my $limit = $opt->{'limit'} ) {
        $qry->setlimit( @$limit );
    }

    # execute query
    my @keys = @{ $qry->search };
    my $rows = +{ map { $_, undef } @keys };
    $rdb->mget($rows);

    return [ map +{ $_ => $rows->{$_} }, @keys ];
}

sub update {
    my $self = shift;

    unless (@_) {
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
    while ( my ($key, $cols) = splice @rows, 0, 2 ) {
        unless ( defined $rdb->put($key, $cols) ) {
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

sub update_or_create {
    my $self = shift;

    unless (@_) {
        Engage::Exception->throw('Specify the keys to update or create');
    }

    my $rdb = $self->rdb('W');
    my @rows;
    my $has_error = 0;

    @rows = @_;
    while ( my ($key, $cols) = splice @rows, 0, 2 ) {
        unless ( defined $rdb->put($key, $cols) ) {
            my $msg = $rdb->errmsg($rdb->ecode);
            $self->log->error(qq/update failed: $msg "$key"/);
            $has_error = 1;
        }
    }

    return !$has_error;
}

1;

__END__
