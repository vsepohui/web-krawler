#!/usr/bin/perl

BEGIN {use FindBin qw($Bin); require "$Bin/_init.pl"};
use 5.024;

use Krawler::Config;
use Redis;
use utf8;

use experimental 'smartmatch';

my $config = Krawler::Config->get;
my $redis = new Redis(server => join (':', $config->{redis}->{host}, $config->{redis}->{port}), encoding => 'utf-8');
my $pref = $config->{'redis_prefix'};


my $url = $ARGV[0] or die 'Url is not specified';
my $html_file = $ARGV[1] or die 'Html file  is not specified';
my $file = $ARGV[2] or die 'Out file  is not specified';

render_url_to_text($url);



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
	open $fo, '>' . $html_file;
	binmode($fo, ":utf8");
	print $fo $html;
	close $fo;


	system("$Bin/../tools/phantomjs-2.1.1-linux-x86_64/bin/phantomjs",  "$Bin/../tmp/phantomjs.script", $html_file, $file);
	
	
	unlink $html_file;

	
	return;
}

1;
