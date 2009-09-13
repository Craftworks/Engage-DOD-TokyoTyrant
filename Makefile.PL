use inc::Module::Install;

name     'Engage-DOD-TokyoTyrant';
all_from 'lib/Engage/DOD/TokyoTyrant.pm';
author   q{Craftworks <craftwork at cpan org>};
license  'perl';

author_tests 'xt';

build_requires 'Test::More';
build_requires 'Test::ttserver';
build_requires 'Test::UseAllModules';

requires 'Moose';
requires 'Moose::Role';
requires 'Engage';
requires 'TokyoTyrant';

auto_install;

WriteAll;
