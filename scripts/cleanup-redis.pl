#!/usr/bin/perl

BEGIN {use FindBin qw($Bin); require "$Bin/_init.pl"};
use 5.024;

use experimental 'smartmatch';

`redis-cli --scan --pattern 'web-krawler:*'|xargs redis-cli del`;

1;
