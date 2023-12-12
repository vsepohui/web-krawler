#!/usr/bin/perl

BEGIN {use FindBin qw($Bin); require "$Bin/_init.pl"};
use 5.024;
use AnyEvent;
use AnyEvent::HTTP;
use Sub::Daemon;
use Krawler::Config;
use URL::Search;
use Redis;

use constant REDIS_PAGE_PREF => 'web-krawler:page:';
use constant REDIS_PAGE_CTIME => 'web-krawler:ctime:';
use constant REDIS_QUEUE => "web-krawler:queue";

use experimental 'smartmatch';

my $cmd = $ARGV[0] || 'help';
$cmd =~ s/\W//g;

my $config = Krawler::Config->get;
my $redis = new Redis;

my $routes = [
	[[qw/worker/],				\&worker],
    [[qw/factory start/], 		\&factory],
    [[qw/help/], 				\&help],
];

(map {$_->[1]->()} grep {$cmd ~~ $_->[0]} @$routes) or help();
exit();


sub help {
    say "Usage:";
    say "\t$! worker   				Start worker process";
    say "\t$! start					Start factory process";
    say "\t$! help					Show this help";
}


sub worker {
	my $port = $ARGV[1];
	unless ($port && $port =~ /^\d+$/) {
		warn "Wrong port";
		return help();
	}

	my $daemon = new Sub::Daemon(
		debug => 0,
		piddir 	=> "$Bin/../pids/",
		logdir 	=> "$Bin/../logs/",
		pidfile => "krawler.worker.$port.pid",
		logfile => "krawler.worker.$port.log",
	);
	$daemon->_daemonize;
	
	$daemon->spawn(
		nproc => 1,
		code  => sub {	
			my $startup_time = scalar localtime;
			
			my $cv = AnyEvent->condvar;
			
			$SIG{$_} = sub { $cv->send } for qw( TERM INT );
			$SIG{'HUP'} = sub {$cv->send};
			
			my $url = $redis->blpop(REDIS_QUEUE);
			http_get $url, sub {
				my $data = shift;
				my $headers = shift;
				return unless $header->{content_type} ~~ ['text/html', 'text/plain'];
				if (length ($data) < 1_00_0000) {
					$redis->set(REDIS_PAGE_PREF.$url => $data);
					$redis->set(REDIS_PAGE_CTIME => time());
					
					my @urls = URL::Search::extract_urls($data);
					for (@urls) {
						$redis->push(REDIS_QUEUE, $_) unless $redis->get(REDIS_PAGE_PREF.$_);
					}
				}
			};
			
			$cv->wait;
		},
	);
}


sub factory {
	my $daemon = new Sub::Daemon(
		debug => 0,
		piddir => "$Bin/../pids/",
		logdir => "$Bin/../logs/",
	);
	$daemon->_daemonize;
	
	my $procs = $config->{ncpus};
	
	for (1..$procs) {
		my $port = PORT_START_FROM() + $_ - 1;
		warn "Start worker on port $port";
		`$Bin/$0 worker $port`;
	}
	
	$redis->push(REDIS_QUEUE, $config->{homepage});

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
