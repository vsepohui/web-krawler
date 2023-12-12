package Krawler::Config;

use 5.024;
use JSON::XS;

use constant CONFIG_PATH => 'krawler.config';



sub _get {
	my $text;
	
	open my $fh, CONFIG_PATH;
	$text = join '', <$fh>;
	close $fh;
	
	return decode_json $text;
}

sub get {
	state $config = _get;
	return $config;
}

1; 
