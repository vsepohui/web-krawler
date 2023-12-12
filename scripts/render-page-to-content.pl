#!/usr/bin/perl

BEGIN {use FindBin qw($Bin); require "$Bin/_init.pl"};
use 5.024;

use Krawler::Config;
use Redis;

use experimental 'smartmatch';

my $config = Krawler::Config->get;
my $redis = new Redis(server => join (':', $config->{redis}->{host}, $config->{redis}->{port}));
my $pref = $config->{'redis_prefix'};

say render_url_to_text('https://anarchy-press.ru/politics');

sub render_url_to_text {
	my $page = shift;
	my $base_url = $page;
	$base_url =~ s/\#.*$//;
	$base_url =~ s/\?.*$//;
	$base_url =~ s/\/[^\/]*$/\//;

	my $key = "$pref:page:$page";
	my $html = $redis->get($key);

	$html =~ s/\<head\>/\<head\>\<base href=\"$base_url\">/;

	my $fo;
	open $fo, '>'. $Bin .'/../tmp/index.html';
	print $fo $html;
	close $fo;

	`cd $Bin/../tmp && $Bin/../tools/phantomjs-2.1.1-linux-x86_64/bin/phantomjs phantomjs.script`;
	
	my $fi;
	open $fi, "$Bin/../tmp/out.txt";
	my $text = join '', <$fi>;
	close $fi;
	
	unlink "$Bin/../tmp/out.txt";
	unlink "$Bin/../tmp/index.html";
	
	return $text;
}

1;
