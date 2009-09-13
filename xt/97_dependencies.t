use strict;
use warnings;
use ExtUtils::MakeMaker;
use Test::Dependencies
    exclude => [qw/Test::Dependencies Engage/],
    style   => 'light';

ok_dependencies();
