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


use constant PORT_START_FROM => 4000;
use constant REDIS_PAGE_PREF => ($config->{'redis_prefix'} . ':page:');
use constant REDIS_TEXT_PREF => ($config->{'redis_prefix'} . ':text:');
use constant REDIS_PAGE_CTIME => ($config->{'redis_prefix'} . ':ctime:');
use constant REDIS_QUEUE => ($config->{'redis_prefix'} . ":queue");
use constant REDIS_RENDER_QUEUE => ($config->{'redis_prefix'} . ":render-queue");
use constant REDIS_LOCK =>( $config->{'redis_prefix'} . ":lock:");


my $redis = AnyEvent::Redis->new(
	host => $config->{redis}->{host},
	port => $config->{redis}->{port},
);

my $redis2 = new Redis(server => join (':', $config->{redis}->{host}, $config->{redis}->{port}), encoding => 'utf-8');

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
    say "	$0 worker   Start worker process";
    say "	$0 start    Start factory process";
    say "	$0 stop     Stop factory process and workers";
    say "	$0 help     Show this help";
}

my %waits = ();
my $cnt = 0;

my %wait2 = ();
my %bad_domain = ();

my $idle = undef;

sub prepare {
	my ($url, $html, $id) = @_;
	return '' unless $html;
	
	my $base_url = $url;
	$base_url =~ s/\#.*$//;
	$base_url =~ s/\?.*$//;
	$base_url =~ s/^(https?:\/\/[^\/]+)$/$1\//;
	$base_url =~ s/\/[^\/]*$/\//;

	$html =~ s/\<head\>/\<head\>\<base href=\"$base_url\">/;
	
	my $f1 = $Bin . "/../tmp/tmp.$id.$$.html";
	my $f2 = $Bin . "/../tmp/tmp.$id.$$.txt";

	my $fo;
	open $fo, '>' . $f1;
	binmode($fo);
	print $fo $html;
	close $fo;

	warn join ' ', "$Bin/../tools/phantomjs-2.1.1-linux-x86_64/bin/phantomjs",  "$Bin/../tmp/phantomjs.script", $f1, $f2;
	system("$Bin/../tools/phantomjs-2.1.1-linux-x86_64/bin/phantomjs",  "$Bin/../tmp/phantomjs.script", $f1, $f2);

	my $result = '';

	my $fi;
	open $fi, $f2;
	binmode($fi);
	$result = join '', <$fi>;
	close $fi;	
	
	unlink $f1;
	unlink $f2;

	
	return $result;

}


sub work {
	my $cv = shift;
	my $id = shift;

	$wait2{$cnt} = $redis->blpop(REDIS_RENDER_QUEUE(), 10, sub {
		my ($p) = @_;

		unless ($p) {
			$cnt --;
			delete $wait2{$cnt};
			return;
		}
		
		my ($q, $url) = @$p;
		
		warn $url;
		if ($url) {
			my $tmp_file = "$Bin/../tmp/tmp.$$.$id.html";
			my $tmp_file2 = "$Bin/../tmp/tmp.$$.$id.txt";
			

			my $pref = $config->{'redis_prefix'};

			my $key = "$pref:page:$url";
			my $html = $redis2->get($key);
			my $text = prepare ($url, $html, $id);

			$redis2->set(REDIS_TEXT_PREF().$url, $text);
			
		}
		delete $wait2{$cnt};
		$cnt --;
	});
	
	$cnt --;
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
		pidfile => "render.worker.$port.pid",
		logfile => "render.worker.$port.log",
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

			my $m = 2;
			my $id = 0;
			
			$idle = AnyEvent->idle(cb => sub {
				if ($is_working) {
					if ($cnt < $m) {
						for (1..($m-$cnt)) {
							$cnt ++;
							work($cv, ++ $id);
						}
					}
				}
			});
			

			for (1..$m) {
				$cnt ++;
				work($cv, ++ $id);
			}
			

			
			$cv->recv;
		},
	);
}

sub stop {
	warn "Stopping render";
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
					open my $fh, "$Bin/../pids/render.worker.$port.pid";
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

