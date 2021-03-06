package Engage::DOD::TokyoTyrant::RDBTBL;

use Moose;
use Encode qw/decode_utf8/;
use Data::Dumper qw/Dumper/;
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

    local $Data::Dumper::Terse = 1;
    my $rdb = $self->rdb('R');

    # has query conditions
    if ( grep ref, @_ ) {
        return $self->search(@_);
    }
    # single
    elsif ( @_ == 1 ) {
        my $key  = shift;
        my $recs = $rdb->get($key);
        return unless $recs;
        my $rv = eval decode_utf8(Dumper [ $key => $recs ]);
        return wantarray ? @$rv : +{ @$rv };
    }
    # multiple
    elsif ( @_ != 1 ) {
        my $recs = +{ map { $_, undef } @_ };
        $rdb->mget($recs);
        return unless %$recs;
        $recs = eval decode_utf8(Dumper $recs);
        return wantarray ? %$recs : $recs;
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
    $rows = eval decode_utf8(Dumper $rows);

    return unless @keys;
    my @rows = map +{ $_ => $rows->{$_} }, @keys;
    return wantarray ? @rows : \@rows;
}

sub update {
    my $self = shift;

    unless (@_) {
        Engage::Exception->throw('Specify the keys to update');
    }

    my $rdb = $self->rdb('W');
    my @rows;
    my $has_error = 0;
    my %data;

    @rows = @_;
    while ( my ($key) = splice @rows, 0, 2 ) {
        unless ( defined ($data{$key} = $rdb->get($key)) ) {
            my $msg = $rdb->errmsg($rdb->ecode);
            $self->log->error(qq/update failed: $msg "$key"/);
            $has_error = 1;
        }
    }
    return 0 if $has_error;

    @rows = @_;
    while ( my ($key, $cols) = splice @rows, 0, 2 ) {
        %$cols = ( %{ $data{$key} }, %$cols );
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

    if ( @_ == 1 && ref $_[0] eq 'HASH' ) {
        my $rows = $self->search(shift);
        @_ = map { keys %$_ } @$rows;
    }

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

sub read_or_create {
    my $self = shift;

    unless (@_) {
        Engage::Exception->throw('Specify the keys to read or create');
    }

    my $rdb = $self->rdb('R');
    my @rows;
    my $has_error = 0;

    my $recs  = +{ @_ };
    my $clone = +{ @_ };

    $rdb->mget($recs);
    my @create = grep { !$recs->{$_} } keys %$clone;
    my %create = map { $_ => $clone->{$_} } @create;

    $self->create(%create);
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

sub mk_key {
    my $self = shift;

    if ( (my $id = $self->rdb('W')->genuid) != -1 ) {
        return $id;
    }

    Engage::Exception->throw(q/Generate a unique ID number failed: $msg/);
}

1;

__END__
