#!/usr/bin/perl
#
package Cluster::Manager;

use Exporter;
use strict;
use warnings;
use Data::Dumper;
use File::Basename;

our $VERSION = 1.0;
our @ISA = qw(Exporter);


sub new{
	my($class) = shift;
	# constructor parameters
	my %obj_attrs = (
		node_list => [],
		local_wd => "",
		cluster_wd => "",
                "cluster_user" => ""
	);
	# override the default value
	my %args = (@_);
	@obj_attrs{keys %args} = values %args;
	my $self = bless \%obj_attrs;
        if($self->{cluster_wd}){
            $self->init_cluster_wd($self->cluster_wd);
        }
        if($self->{local_wd}){
            $self->init_local_wd($self->local_wd);
        }
        # node_list and cluster_user is required
        if(!$self->{cluster_user} or !$self->{node_list}){
            my $usage = <<EOF;
new ClusterManager(
    node_list => qw(host1,host2),
    cluster_user => "user login cluster node"
    [,local_wd => "/local/work/dir" cluster_wd => "/cluster/wd"]
EOF
            die $usage;

        }

	return $self;
}

sub init_local_wd{
    my $self = shift;
    my $dir = shift;
    if(not -d $dir){
        print "[info] create local working directory: $dir\n";
        $self->execute_on_local("mkdir -p $dir");
    }
}

sub init_cluster_wd{
    my($self,$dir) = @_;
    my $cmd = "[ ! -d $dir ] && mkdir -p $dir";
    $self->execute_on_cluster($cmd);
}


sub set_cluster_user{
    my($self,$cluster_user) = @_;
    $self->{cluster_user} = $cluster_user;
}

sub node_file_exist{
    my ($self, $node_idx, $file, $is_dir) = @_;
    my $test_op = $is_dir ? "-d" : "-f";
    return $self->execute_on_node($node_idx, "[ $test_op $file ] && echo exist");
}

sub local_file_exist{
    my($self,$file, $is_dir) = @_;
    my $test_op = $is_dir ? "-d" : "-f";
    return $self->execute_on_local("[ $test_op $file ] && echo exist");
}


sub cluster_cat{
    my $self = shift;
    my $node_file_pat = shift;
    my $node_file_arg;
    if(@_ == 2){
        $node_file_arg = shift;
    }
    my $local_file = shift;
    foreach my $i ( 0 .. @{$self->{node_list}} - 1){
        my $node_file = $node_file_arg ? sprintf($node_file_pat, $node_file_arg->[$i]) : $node_file_pat;
        $self->node_cat($i,$node_file,$local_file);
    }
}



# concatenate a remote file to local one
sub node_cat{
    my($self,$node, $node_file, $local_file) = @_;
    $self->execute_on_node($node, "cat $node_file", ">> $local_file");
}


sub rsync_from_node{
    my($self,$node_idx,$node_path, $local_path) = @_;
    my $node = $self->{node_list}->[$node_idx];
    my $cmd = "rsync -avrz $self->{cluster_user}\@$node:$node_path $local_path 1>/dev/null 2>&1";
    _execute_cmd($cmd);
}

sub rsync_to_node{
    my($self,$node_idx,$local_path, $node_path) = @_;
    my $node = $self->{node_list}->[$node_idx];
    my $cmd = "rsync -avrz $local_path $self->{cluster_user}\@$node:$node_path 1>/dev/null 2>&1";
    $self->execute_on_local($cmd);
}

sub rsync_to_cluster{
    my ($self,$local_path_pat,@rest) = @_;
    my $local_path_arg;
    if(@rest == 2){
        $local_path_arg = shift @rest;
    }
    my $node_path = shift @rest;
    
    foreach my $i( 0 .. @{$self->{node_list}} - 1){
        my $local_path = $local_path_arg ? sprintf($local_path_pat,$local_path_arg->[$i]) : $local_path_pat;
        $self->rsync_to_node($i,$local_path,$node_path);
    }
}

sub split_and_distribute{
    my($self, $local_file,$cluster_path) = @_;

    my $local_dir = dirname($local_file);
    my $local_file_name = basename($local_file);

    my $old_local_wd = $self->{local_wd};
    $self->{local_wd} = $local_dir;
    my $split_prefix = "$local_file_name" . "_";
    my $num_of_nodes = @{$self->{node_list}};
    my $num_of_lines = $self->execute_on_local("wc -l $local_file|grep -o -P \"^\\d+\"");
    my $num_per_node = int($num_of_lines / $num_of_nodes) + 1;
    my $cmd = "split -d -l $num_per_node $local_file $split_prefix";
    $self->execute_on_local($cmd);
    # now distribute to cluster node
    my $local_split_pat = $split_prefix . "%02d";
    my $local_split_arg = [0 .. $num_of_nodes -1];
    $self->rsync_to_cluster($local_split_pat,$local_split_arg,$cluster_path);
    $self->{local_wd} = $old_local_wd;
}


# add a cluster node
sub add_node{
	my $self = shift;
	my ($addr) = @_;
	push @{$self->{node_list}}, $addr;
	return $self;
}

sub execute_on_local{
    my ($self,$cmd) = @_;
    if($self->{local_wd}){
        $cmd = "cd $self->{local_wd}; $cmd";
    }
    return _execute_cmd($cmd);
}

sub execute_on_node{
    my $self = shift;
    my($node_idx, $cmd, $redir_option) = @_;
    my $node_addr = $self->{node_list}->[$node_idx];
    # set the working dir if it is specified
    if($self->{cluster_wd}){
        $cmd = "cd $self->{cluster_wd}; $cmd";
    }
    $redir_option or $redir_option = "";
    my $ssh_cmd = "ssh $self->{cluster_user}\@$node_addr '$cmd' $redir_option";
    return _execute_cmd($ssh_cmd);
}

# set the remote command background mode
# which means it will return immediately with the pid
sub execute_on_node_bg{
    my $self = shift;
    my($node_idx, $cmd, $log_file) = @_;
    $log_file or $log_file = "/dev/null";
    my $node_addr = $self->{node_list}->[$node_idx];
    # set the working dir if it is specified
    if($self->{cluster_wd}){
        $cmd = "cd $self->{cluster_wd}; $cmd";
    }
    my $ssh_cmd = "ssh $self->{cluster_user}\@$node_addr '$cmd 1>$log_file 2>&1 & echo \$!' ";
    return _execute_cmd($ssh_cmd);
}

sub execute_on_cluster{
    my($self, $cmd_pattern, $cmd_args) = @_;
    my $execute_result_map = {};

    foreach my $i( 0 .. @{$self->{node_list}} - 1){
        my $cmd = $cmd_args? sprintf($cmd_pattern, $cmd_args->[$i]) : $cmd_pattern;
        $execute_result_map->{$i} = $self->execute_on_node($i,$cmd);
    }
    return $execute_result_map;
}

sub execute_on_cluster_bg{
    my $self = shift;
    my %expect_args = (
        cmd_pattern => undef,
        cmd_args => undef,
        log_file => undef,
    );

    my %args = @_;
    @expect_args{keys %args} = values %args;
    # modify the command line by setting it to background and return the pid
    my $execute_result_map = {};
    my $cmd_pattern = $expect_args{cmd_pattern};
    my $cmd_args = $expect_args{cmd_args};
    my $log_file = $expect_args{log_file};
    foreach my $i( 0 .. @{$self->{node_list}} - 1){
        my $cmd = $cmd_args ? sprintf($cmd_pattern, $cmd_args->[$i]) : $cmd_pattern;
        $execute_result_map->{$i} = $self->execute_on_node_bg($i,$cmd, $log_file);
    }
    return $execute_result_map;

}

sub wait_cluster_execute{
    my($self,$execute_pid_map) = @_;
    my $num_processes = keys %$execute_pid_map;
    while($num_processes > 0){
        while(my($node_idx,$pid) = each %$execute_pid_map){
            if(not $self->check_node_pid($node_idx,$pid)){
                delete $execute_pid_map->{$node_idx};
                $num_processes --;
            }
        }
        print ".";
        sleep(5);
    }
    print "\n [info] all processes return\n";
}

sub check_node_process{
    my($self,$node_idx, $pname) = @_;
    my $cmd = "ps ax|grep -o -P '$pname'";
    return $self->execute_on_node($node_idx,$cmd);
}

sub check_node_pid{
    my($self,$node_idx, $pid) = @_;
    my $cmd = "ps cax|grep -o -P '^\\s*$pid'";
    return $self->execute_on_node($node_idx,$cmd);
}


sub dump{
    my $self = shift;
    print Dumper($self);
}

sub _execute_cmd{
    my($cmd) = @_;
    print "[info] $cmd\n";
    my $output = `$cmd`;
    chomp $output;
    return $output;
}
