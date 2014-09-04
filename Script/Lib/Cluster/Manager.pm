#!/usr/bin/perl
#
package Cluster::Manager;

use Exporter;
use strict;
use warnings;

our $VERSION = 1.0;
our @ISA = qw(Exporter);


sub new{
	my($class) = {};
	# constructor parameters
	my %obj_attrs = (
		node_list => {},
		local_wd => "",
		remote_wd => ""
	);
	# override the default value
	my %args = (@_);
	@obj_attrs{keys %args} = values %args;
	my $self = bless $obj_attrs;
	return $self;
}

# add a cluster node
sub add_node{
	my $self = shift;
	my ($alias,$addr) = @_;
	$self->{node_list}->{$alias} = $addr;
	return $self;
}




