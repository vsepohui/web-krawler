#!/usr/bin/perl

our $config;
BEGIN {
	use FindBin qw($Bin); 
	require "$Bin/_init.pl";
	use lib "$Bin/../lib";
	use Krawler::Config;
	$config = Krawler::Config->get;
};

use 5.024;
use AnyEvent;
use AnyEvent::HTTP;
use Sub::Daemon;
use URL::Search;
use HTML::LinkExtor;
use Redis;
use AnyEvent::Redis;
use List::Uniq qw/uniq/;
use URI;
use utf8;



use experimental 'smartmatch';

$AnyEvent::HTTP::MAX_PER_HOST = 8;

my $cmd = $ARGV[0] || 'help';
$cmd =~ s/\W//g;


use constant PORT_START_FROM => 3000;
use constant REDIS_PAGE_PREF => ($config->{'redis_prefix'} . ':page:');
use constant REDIS_PAGE_CTIME => ($config->{'redis_prefix'} . ':ctime:');
use constant REDIS_QUEUE => ($config->{'redis_prefix'} . ":queue");
use constant REDIS_LOCK =>( $config->{'redis_prefix'} . ":lock:");

my $redis = AnyEvent::Redis->new(
	host => $config->{redis}->{host},
	port => $config->{redis}->{port},
);

my $redis2 = new Redis(server => join (':', $config->{redis}->{host}, $config->{redis}->{port}));

my $routes = [
	[[qw/worker/],				\&worker],
	[[qw/factory start/], 		\&factory],
	[[qw/stop/], 				\&stop],	
	[[qw/help/], 				\&help],
];

(map {$_->[1]->()} grep {$cmd ~~ $_->[0]} @$routes) or help();


exit;

sub help {
    say "Usage:";
    say "	$0 worker   				Start worker process";
    say "	$0 start					Start factory process";
    say "	$0 stop						Stop factory process and workers";
    say "	$0 help						Show this help";
}

my %waits = ();
my $cnt = 0;

my %wait2 = ();
my %bad_domain = ();

my $idle = undef;

sub work {
	my $cv = shift;

	$wait2{$cnt} = $redis->blpop(REDIS_QUEUE(), 10, sub {
		delete $wait2{$cnt};

		my ($p) = @_;
		unless ($p) {
			$cnt --;
			return;
		}
		my ($q, $url) = @$p;
		

					
		unless ($url) {
			warn "No url";
			$cnt --;
			return;
		}

		$waits{$url} = http_request(
			GET => $url, 
			headers => { "user-agent" => "MySearchClient 1.0" },
			timeout => 10,
			sub {
					$cnt --;
				delete $waits{$url};
				$redis2->hdel(REDIS_LOCK(),$url);
				my $uri = URI->new($url);
				my $host = $uri->host;
				my $host2 = $host;
				my ($proto) = $url =~ /^(\w+):\/\//;
				my $url_base = $url;

				my $data = shift;
				my $headers = shift;
				use Data::Dumper;
				#warn Dumper ($headers);

				if ($headers->{Status} eq '506') {
					#$redis2->set(REDIS_PAGE_CTIME().$url => time());
					$redis2->hset(REDIS_LOCK(), $url => 1);
					$bad_domain{$uri->host}++;
#					work();
					return;
				} elsif ($headers->{Status} ne '200') {
					$redis2->hset(REDIS_LOCK(), $url => 1);
					$bad_domain{$uri->host}++;
#					work();
					return;
				}

				my $type = $headers->{'content-type'};
				$type =~ s/;.+$//;
				unless ($type ~~ ['text/html', 'text/plain']) {
					#$cv->send unless keys %wait2;
					return;
				}
				if (length ($data) < 1_00_0000) {
					#warn "Data $url => " . $data;
					$redis2->set(REDIS_PAGE_PREF().$url => $data);
					$redis2->set(REDIS_PAGE_CTIME().$url => time());
					
					my @as;
					my $p = HTML::LinkExtor->new(sub {
						my($tag, %attr) = @_;
						return if $tag ne 'a';  # we only look closer at <img ...>
						push(@as, values %attr);
					});
					
					my @urls = uniq (URL::Search::extract_urls($data));
					$p->parse($data);

					for my $url (grep {$_} @as) {
						next if $url =~ /^javascript\s*:/;
						if ($url =~ /^\//) {
							$url = "$proto:\/\/$host2" . $url;
						} elsif ($url =~ /^https?:\/\// ) {
							1;
						} elsif ($url =~ /^(tg|mailto):\/\// ) {
							next;
						} elsif ($url =~ /^\/\// ) {
							$url = $proto . ':' . $url;
						} elsif ($url =~ /^[\w\d_\.]/ ) {
							$url = $url_base . $url;
						} elsif ($url =~ /^\#/) {
							next;
						} elsif ($url =~ /^\?/) {
							$url = $url_base . $url;
						} else {
							warn "Wrong url $url";
							next;
						}
						my $uri = URI->new($url);
						my $host = $uri->host;
							
						
						next if ($bad_domain{$host} > 10);
						next if $url =~ /^https?\:\/\/www\.w3\.org\//;
						next if $url =~ /\.(js|css|png|jpeg|jpg|gif|mp3|webp|font|ttf)\??/;
						unless ($redis2->get(REDIS_PAGE_PREF.$url) && $redis2->hget(REDIS_LOCK(),$url)) {
							$redis2->hset(REDIS_LOCK(), $url => 1);
							$redis2->rpush(REDIS_QUEUE(), $url);
						}
					}
				}
				#work();
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
			my $is_working = 1;
			
			$SIG{$_} = sub {$is_working = 0; $cv->send } for qw( TERM INT );
			$SIG{'HUP'} = sub {$is_working = 0; $cv->send};

			my $m = $config->{max_requests_per_one_process};
			for (1..$m) {
				work($cv);
				$cnt ++;
			}
			
			$idle = AnyEvent->idle(cb => sub {
				if ($is_working) {
					if ($cnt < $m) {
						for (1..($m-$cnt)) {
							work($cv);
							$cnt ++;
						}
					}
				}
			});
			
			$cv->recv;
		},
	);
}

sub stop {
	warn "Stopping krawler";
	`cat $Bin/../pids/$0.pid|xargs kill`;
	exit;
}


sub factory {
	my $daemon = new Sub::Daemon(
		debug => 0,
		piddir => "$Bin/../pids/",
		logdir => "$Bin/../logs/",
	);
	$daemon->_daemonize;
	
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
