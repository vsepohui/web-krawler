#!/usr/bin/perl

BEGIN {use FindBin qw($Bin); require "$Bin/_init.pl"};
use 5.024;

use Krawler::Config;
use Redis;

use experimental 'smartmatch';

my $config = Krawler::Config->get;
my $redis = new Redis(server => join (':', $config->{redis}->{host}, $config->{redis}->{port}));

my $pref = $config->{'redis_prefix'};
for (split /\n/, `redis-cli --scan --pattern '$pref:*'`) {
	$redis->del($_);
}

1;
