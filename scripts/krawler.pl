#!/usr/bin/perl

BEGIN {use FindBin qw($Bin); require "$Bin/_init.pl"};
use 5.024;
use AnyEvent;
use AnyEvent::HTTP;
use Sub::Daemon;
use Krawler::Config;
use URL::Search;
use HTML::LinkExtor;
use Redis;
use AnyEvent::Redis;
use List::Uniq qw/uniq/;
use URI;
use utf8;

use constant PORT_START_FROM => 3000;
use constant REDIS_PAGE_PREF => 'web-krawler:page:';
use constant REDIS_PAGE_CTIME => 'web-krawler:ctime:';
use constant REDIS_QUEUE => "web-krawler:queue";
use constant REDIS_LOCK => "web-krawler:lock:";

use experimental 'smartmatch';

my $cmd = $ARGV[0] || 'help';
$cmd =~ s/\W//g;

my $config = Krawler::Config->get;
my $redis = AnyEvent::Redis->new(
  host => '127.0.0.1',
  port => '6379',
);


my $redis2 = new Redis;
my $routes = [
	[[qw/worker/],				\&worker],
    [[qw/factory start/], 		\&factory],
    [[qw/help/], 				\&help],
];

(map {$_->[1]->()} grep {$cmd ~~ $_->[0]} @$routes) or help();
exit();


sub help {
    say "Usage:";
    say "	$0 worker   				Start worker process";
    say "	$0 start					Start factory process";
    say "	$0 help					Show this help";
}

my %waits = ();
my $cnt = 0;

my %wait2 = ();
my %bad_domain = ();



sub work {
	my $cv = shift;
	$cnt ++;

	warn "Startig BLPOP";
	$wait2{$cnt} = $redis->blpop(REDIS_QUEUE(), 300, sub {
		delete $wait2{$cnt};
		my ($p) = @_;
		my ($q, $url) = @$p;;
		
		use Data::Dumper;
		warn "URL => ".Dumper \@_;
					
		unless ($url) {
			warn "No url";

			return;
		}

		warn "Fetching $url";
	
		$waits{$url} = http_request(
			GET => $url, 
			headers => { "user-agent" => "MySearchClient 1.0" },
			timeout => 10,
			sub {
				delete $waits{$url};
				$redis2->hdel(REDIS_LOCK(),$url);
				my $uri = URI->new($url);
				my $host = $uri->host;
				my $host2 = $host;
				my ($proto) = $url =~ /^(\w+):\/\//;
				my $url_base = $url;
						
				warn "Fetched url $url";
				my $data = shift;
				my $headers = shift;
				use Data::Dumper;
				warn Dumper ($headers);

				if ($headers->{Status} eq '595') {
					$bad_domain{$uri->host}++;
				}

				my $type = $headers->{'content-type'};
				$type =~ s/;.+$//;
				unless ($type ~~ ['text/html', 'text/plain']) {
					$cv->send unless keys %wait2;
					return;
				}
				if (length ($data) < 1_00_0000) {
					warn "Data $url => " . $data;
					$redis2->set(REDIS_PAGE_PREF().$url => $data);
					$redis2->set(REDIS_PAGE_CTIME() => time());
					
					my @as;
					sub callback {
						my($tag, %attr) = @_;
						return if $tag ne 'a';  # we only look closer at <img ...>
						push(@as, values %attr);
					}
					
					my $p = HTML::LinkExtor->new(\&callback);
					
					my @urls = uniq (URL::Search::extract_urls($data));
					$p->parse($data);
					warn Dumper \@as;
					for my $url (map {$_->{href}} @as) {
						if ($url =~ /^\//) {
							$url = "$proto:\/\/$host2" . $url;
						} elsif ($url =~ /^\https?:\/\// ) {
							1;
						}  elsif ($url =~ /^\/\// ) {
							$url = $proto . ':' . $url;
						} elsif ($url =~ /^\[.]/ ) {
							$url = $url_base . $url;
						}
						my $uri = URI->new($url);
						my $host = $uri->host;
							
						
						next if ($bad_domain{$host} > 10);
						next if $url =~ /^https?\:\/\/www\.w3\.org\//;
						next if $url =~ /\.(js|css|png|jpeg|jpg|gif|mp3|webp|font|ttf)\??/;
						warn "Found url: $url";
						unless ($redis2->get(REDIS_PAGE_PREF.$url) && $redis2->hget(REDIS_LOCK(),$url)) {
							$redis2->hset(REDIS_LOCK(), $url => 1);
							warn "rpush(REDIS_QUEUE(), $url)";
							$redis2->rpush(REDIS_QUEUE(), $url);
						}
					}
				}
				$cv->send unless keys %wait2;
			},
		);
	});
}


sub worker {
	my $port = $ARGV[1];
	unless ($port && $port =~ /^\d+$/) {
		warn "Wrong port";
		return help();
	}

	my $daemon = new Sub::Daemon(
		debug => 1,
		piddir 	=> "$Bin/../pids/",
		logdir 	=> "$Bin/../logs/",
		pidfile => "krawler.worker.$port.pid",
		logfile => "krawler.worker.$port.log",
	);
	#$daemon->_daemonize;
	
	$daemon->spawn(
		nproc => 1,
		code  => sub {	
			my $startup_time = scalar localtime;
			
			my $cv = AnyEvent->condvar;
			
			$SIG{$_} = sub { $cv->send } for qw( TERM INT );
			$SIG{'HUP'} = sub {$cv->send};

			work($cv) for 1..$config->{max_requests_per_one_process};
			
			$cv->recv;
		},
	);
}


sub factory {
	my $daemon = new Sub::Daemon(
		debug => 1,
		piddir => "$Bin/../pids/",
		logdir => "$Bin/../logs/",
	);
	#$daemon->_daemonize;
	
	my $procs = $config->{ncpus};
	
	$redis2->rpush(REDIS_QUEUE(), $config->{homepage});
	
	for (1..$procs) {
		my $port = PORT_START_FROM() + $_ - 1;
		warn "Start worker on port $port";
		`$Bin/$0 worker $port`;
	}
	
	
	
	

	$daemon->spawn(
		nproc => 1,
		code  => sub {	
			my $cv = AnyEvent->condvar;
			
			my $get_worker_pids = sub {
				my @pids = ();
				for (1..$procs) {
					my $port = PORT_START_FROM() + $_ - 1;
					open my $fh, "$Bin/../pids/krawler.worker.$port.pid";
					my $pid = <$fh>;
					chomp $pid;
					close $fh;
					push @pids, $pid;
				}
				return @pids;
			};
			
			
			for my $sig_type (qw(TERM INT)) {
				my @pids = $get_worker_pids->();
				$SIG{$sig_type} = sub {
					for my $pid (@pids) {
						kill $sig_type, $pid;
					}
					$cv->send();
				};
			}
			
			$SIG{'HUP'} = sub {
				my @pids = $get_worker_pids->();
				for my $pid (@pids) {
					kill 'HUP', $pid;
				}
			};	
			
			$cv->recv;
		}
	);
	
	return 1;
}

1;
