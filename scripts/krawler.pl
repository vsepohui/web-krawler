#!/usr/bin/perl

BEGIN {use FindBin qw($Bin); require "$Bin/_init.pl"};
use 5.024;
use AnyEvent;
use AnyEvent::HTTP;
use Sub::Daemon;
use Krawler::Config;
use URL::Search;
use Redis;
use AnyEvent::Redis;
use List::Uniq qw/uniq/;

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

sub work {
	$cnt ++;
	$redis->lpop(REDIS_QUEUE(), sub {
		my ($url) = (@_);
		unless ($url) {
			warn "No url";
			sleep 1;
			return;
		}

		warn "Fetching $url";
	
		$waits{$url} = http_request(
			GET => $url, 
			headers => { "user-agent" => "MySearchClient 1.0" },
			sub {
				delete $waits{$url};
				work() if $cnt < 8000;
				$redis2->hdel(REDIS_LOCK(),$url);
				warn "Fetched url $url";
				my $data = shift;
				my $headers = shift;
				use Data::Dumper;
				warn Dumper ($headers);
				my $type = $headers->{'content-type'};
				$type =~ s/;.+$//;
				return unless $type ~~ ['text/html', 'text/plain'];
				if (length ($data) < 1_00_0000) {
					warn "Data $url => " . $data;
					$redis2->set(REDIS_PAGE_PREF().$url => $data);
					$redis2->set(REDIS_PAGE_CTIME() => time());
					
					my @urls = uniq (URL::Search::extract_urls($data));
					for my $url (@urls) {
						warn "Found url: $url";
						unless ($redis2->get(REDIS_PAGE_PREF.$url) && $redis2->hget(REDIS_LOCK(),$url)) {
							$redis2->hset(REDIS_LOCK(), $url => 1);
							warn "rpush(REDIS_QUEUE(), $url)";
							$redis2->rpush(REDIS_QUEUE(), $url);
						}
					}
				}
			},
		);
		work() if $cnt < 8000;
	});
	#
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

			work();
			
			$cv->wait;
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
