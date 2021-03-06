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


sub new {
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
        if($self->{local_wd}){
            $self->init_local_wd($self->{local_wd});
        }
        if($self->{cluster_wd}){
            $self->init_cluster_wd($self->{cluster_wd});
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
        $self->execute_on_local("mkdir -p $dir", no_lcd => 1);
    }
    $self->{local_wd} = $dir;
}

sub init_cluster_wd{
    my($self,$dir) = @_;
    my $cmd = "[ ! -d $dir ] && mkdir -p $dir";
    $self->execute_on_cluster(cmd_pattern => $cmd, no_lcd => 1, no_rcd=>1);
    $self->{cluster_wd} = $dir;
}


sub set_cluster_user{
    my($self,$cluster_user) = @_;
    $self->{cluster_user} = $cluster_user;
}

sub node_file_exist{
    my ($self, $node_idx, $file, %rest_args) = @_;
    my %default_args = (
	    is_dir => 0
    );
    move_hash_values(\%rest_args,\%default_args,[qw(is_dir)]);
    my $is_dir = $default_args{is_dir};
    my $test_op = $is_dir ? "-d" : "-f";
    return $self->execute_on_node($node_idx, "[ $test_op $file ] && echo 1", %rest_args);
}

sub local_file_exist{
    my($self,$file, %rest_args) = @_;
    my %default_args = (
	    is_dir => 0
    );
    move_hash_values(\%rest_args,\%default_args,[qw(is_dir)]);
    my $is_dir = $default_args{is_dir};
    my $test_op = $is_dir ? "-d" : "-f";
    return $self->execute_on_local("[ $test_op $file ] && echo exist",%rest_args);
}


sub cluster_cat{
    my $self = shift;
    my %default_args = (
	    remote_path_pattern => undef,
	    remote_path_args => undef,
	    local_path => undef
    );
    my %rest_args = @_;
    move_hash_values(\%rest_args,\%default_args,[keys %default_args]);
    my $remote_file_pattern = $default_args{remote_path_pattern};
    my $remote_file_args = $default_args{remote_path_args};
    my $local_file = $default_args{local_path};
    $local_file and $remote_file_pattern or  die "remote file and local file must exist";

    foreach my $i ( 0 .. @{$self->{node_list}} - 1){
        my $node_file = $remote_file_args ? sprintf($remote_file_pattern, $remote_file_args->[$i]) : $remote_file_pattern;
        $self->node_cat($i,$node_file,$local_file, %rest_args);
    }
}


# concatenate a remote file to local one
sub node_cat{
    my($self,$node, $node_file, $local_file, %rest_args) = @_;
    $self->execute_on_node($node, "cat $node_file", redir_option => ">> $local_file", %rest_args);
}


sub rsync_from_node{
    my($self,$node_idx,$node_path, $local_path, %rest_args) = @_;
    my $node = $self->{node_list}->[$node_idx];
    if($node_path !~ /^\//){
	    $node_path = $self->{cluster_wd} . "/" . $node_path;
    }
    my $cmd = "rsync -avrz $self->{cluster_user}\@$node:$node_path $local_path 1>/dev/null 2>&1";
    $self->execute_on_local($cmd,%rest_args);
}

sub rsync_to_node{
    my($self,$node_idx,$local_path, $node_path, %rest_args) = @_;
    my $node = $self->{node_list}->[$node_idx];
    if(not $self->node_file_exist($node_idx, $node_path,is_dir => 1)){
	    print "[warn] remote path does not exist: $node_path, it will be created\n";
	    $self->execute_on_node($node_idx, "mkdir -p $node_path");
    }
    if($node_path !~ /^\//){
	    $node_path = $self->{cluster_wd} . "/" . $node_path;
    }
    my $cmd = "rsync -avrz $local_path $self->{cluster_user}\@$node:$node_path 1>/dev/null 2>&1";
    $self->execute_on_local($cmd,%rest_args);
}

sub rsync_to_cluster{
    my ($self,%rest_args) = @_;
    my %default_args = (
	    local_path_pattern => undef,
	    local_path_args => undef,
	    remote_path => undef
    );
    move_hash_values(\%rest_args,\%default_args,[qw(local_path_pattern local_path_args remote_path)]);
    my $local_path_pattern = $default_args{local_path_pattern};
    my $local_path_args = $default_args{local_path_args};
    my $remote_path = $default_args{remote_path};
    defined $local_path_pattern and defined $remote_path or die "local path and remote path must be provided";
    foreach my $i( 0 .. @{$self->{node_list}} - 1){
        my $local_path = $local_path_args ? sprintf($local_path_pattern,$local_path_args->[$i]) : $local_path_pattern;
        $self->rsync_to_node($i,$local_path,$remote_path,%rest_args);
    }
}



sub check_cluster_pname{
	my ($self,$pname) = @_;
	my $cluster_pid_map = ();
	foreach my $node_idx ( 0 .. @{$self->{node_list}} - 1){
		my $node_pid_map = $self->check_node_pname($node_idx,$pname);
		if($node_pid_map){
			foreach my $pid(keys %$node_pid_map){
				$cluster_pid_map->{$node_idx}->{$pid} = 1;
			}
		}
	}
	return $cluster_pid_map;
}

sub all_nodes_true {
	my $self = shift;
	my $node_result_map = shift;
	my $true_cnt = 0;
	map {$true_cnt += ($node_result_map->{$_} ? 1 :0) } keys %$node_result_map;
	return $true_cnt == scalar @{$self->{node_list}};
}

my @LETTER_SEQ = qw(aa ab ac ad ae af ag ah ai aj ak al);

sub split_and_distribute{
    my($self, $local_file,$cluster_path,%rest_args) = @_;
    # a relative path
    if($local_file !~ m/^\//){
	    $local_file = $self->{local_wd} . "/" . $local_file;
    }
    if(not -f $local_file){
	    print "[warn] local file: $local_file does not exist\n";
	    return;
    }
    # make sure the file exists
    my %default_args = (
        split_prefix => undef,
        use_number => 0,
    );

    move_hash_values(\%rest_args,\%default_args,[keys %default_args]);
    my $local_dir = dirname($local_file);
    my $local_file_name = basename($local_file);
    my $old_local_wd = $self->{local_wd};
    $self->{local_wd} = $local_dir;
    my $num_of_nodes = @{$self->{node_list}};
    my $num_of_lines = $self->execute_on_local("wc -l $local_file|grep -o -P \"^\\d+\"");
    my $num_per_node = int($num_of_lines / $num_of_nodes) + 1;
    my $use_num_opt = $default_args{use_number} ? "-d" : "";
    my $split_prefix = $default_args{split_prefix} ? $default_args{split_prefix} : $local_file_name . "_";
    my $cmd = "split $use_num_opt -l $num_per_node $local_file $split_prefix";
    $self->execute_on_local($cmd);
    # now distribute to cluster node
    my $local_split_pat;
    my $local_split_arg;
    if($default_args{use_number}){
        $local_split_pat = $split_prefix . "%02d";
        $local_split_arg = [0 .. $num_of_nodes -1];
    }else{
        $local_split_pat = $split_prefix . "%s";
        $local_split_arg = [@LETTER_SEQ[0 .. $num_of_nodes - 1]];
    }
    $self->rsync_to_cluster(local_path_pattern => $local_split_pat, local_path_args => $local_split_arg, 
        remote_path => $cluster_path, %rest_args);
    $self->{local_wd} = $old_local_wd;
}



# add a cluster node
sub add_node{
    my $self = shift;
    my ($addr) = @_;
    push @{$self->{node_list}}, $addr;
    return $self;
}


sub combine_multiple_command{
    return join(";", @_);
}

sub execute_on_local{
    my ($self,$cmd,%rest_args) = @_;
    my %default_args = (no_lcd => 0);
    move_hash_values(\%rest_args,\%default_args,[keys %default_args]);
    if($self->{local_wd} and !$default_args{no_lcd}){
        $cmd = "cd $self->{local_wd}; $cmd";
    }
    return _execute_cmd(($cmd,%rest_args));
}

sub execute_on_node{
    my($self,$node_idx,$cmd,%rest_args) = @_;
    my %default_args = (
        redir_option => "",
	no_lcd => 0,
	no_rcd => 0
    );
    move_hash_values(\%rest_args,\%default_args,[qw(redir_option no_lcd no_rcd)]);
    my $redir_option = $default_args{redir_option};
    my $node_addr = $self->{node_list}->[$node_idx];
    # set the working dir if it is specified
    if($self->{cluster_wd} and not $default_args{no_rcd}){
        $cmd = "cd $self->{cluster_wd}; $cmd";
    }
    my $ssh_cmd = "ssh $self->{cluster_user}\@$node_addr '$cmd' $redir_option";
    if($self->{local_wd} and not $default_args{no_lcd}){
	    $ssh_cmd = "cd $self->{local_wd}; " . $ssh_cmd;
    }
    return _execute_cmd(($ssh_cmd,%rest_args));
}

# set the remote command background mode
# which means it will return immediately with the pid
sub execute_on_node_bg{
    my $self = shift;
    my $node_idx = shift;
    my $cmd = shift;
    my %rest_args = @_;
    my %default_args = (
        log_file => "/dev/null"
    );
    move_hash_values(\%rest_args,\%default_args,[qw(log_file)]);
    my $log_file = $default_args{log_file};
    my $redir_option = "";
    if(not $log_file eq ""){
	    $redir_option = "1>$log_file 2>&1";
    }
    my $node_addr = $self->{node_list}->[$node_idx];
    # set the working dir if it is specified
    if($self->{cluster_wd}){
        $cmd = "cd $self->{cluster_wd}; $cmd";
    }
    my $ssh_cmd = "ssh $self->{cluster_user}\@$node_addr '$cmd $redir_option & echo \$!' ";
    return _execute_cmd(($ssh_cmd,%rest_args));
}

sub execute_on_cluster{
    my $self = shift;
    my %default_args = (
        cmd_pattern => undef,
        cmd_args => undef
    );
    my %rest_args = (@_);
    move_hash_values(\%rest_args,\%default_args,[qw(cmd_pattern cmd_args)]);
    my $cmd_pattern = $default_args{cmd_pattern};
    my $cmd_args = $default_args{cmd_args};
    my $execute_result_map = {};
    foreach my $i( 0 .. @{$self->{node_list}} - 1){
	    my $cmd = $cmd_args? sprintf($cmd_pattern, _extract_cmd_args($cmd_args,$i)) : $cmd_pattern;
	    $execute_result_map->{$i} = $self->execute_on_node($i,$cmd,%rest_args);
    }
    return $execute_result_map;
}

sub _extract_cmd_args{
	my($args, $idx) = @_;
	if(!ref($args->[0])){
		return ($args->[$idx]);
	}else{
		my @result = ();
		foreach my $tmp_arg (@$args){
			push @result, $tmp_arg->[$idx];
		}
		return @result;
	}
}

sub move_hash_values{
    my ($from_hash,$to_hash,$keys) = @_;
    foreach my $key(@$keys){
        if(exists $from_hash->{$key}){
            $to_hash->{$key} = $from_hash->{$key};
            delete $from_hash->{$key};
        }
    }
}

sub execute_on_cluster_bg{
    my $self = shift;
    my %default_args = (
        cmd_pattern => undef,
        cmd_args => undef,
    );
    my %rest_args = @_;
    move_hash_values(\%rest_args,\%default_args,[qw(cmd_pattern cmd_args)]);
    $default_args{cmd_pattern} or die "command pattern must exist";
# modify the command line by setting it to background and return the pid
    my $host_pid_map = {};
    my $cmd_pattern = $default_args{cmd_pattern};
    my $cmd_args = $default_args{cmd_args};
    foreach my $i( 0 .. @{$self->{node_list}} - 1){
        my $cmd = $cmd_args ? sprintf($cmd_pattern, _extract_cmd_args($cmd_args,$i)) : $cmd_pattern;
        my $pid = $self->execute_on_node_bg($i,$cmd,%rest_args);
        $host_pid_map->{$i}->{$pid} = 1;
    }
    return $host_pid_map;
}

sub sync_cluster_execute{
    my ($self, @args) = @_;
    my $cluster_pid_map = $self->execute_on_cluster_bg(@args);
    $self->wait_cluster_pid($cluster_pid_map);
}

sub wait_cluster_pid{
    my($self,$execute_pid_map) = @_;
    my $num_nodes_alive = keys %$execute_pid_map;
    print "[info] wait cluster processes to finish, could take long time\n";
    while($num_nodes_alive > 0){
        while(my($node_idx,$pid_map) = each %$execute_pid_map){
            if($self->node_pid_finish($node_idx,$pid_map)){
                $num_nodes_alive--;
		delete $execute_pid_map->{$node_idx};
                print "[info] processes on node-$node_idx are done, $num_nodes_alive more nodes active\n";
            }
        }
        print ".";
        $num_nodes_alive and sleep(5);
    }
    print "\n[info] all nodes done\n";
}

sub wait_node_pid{
	my($self,$node_idx,$node_pid_map,%rest_args) = @_;
	my %default_args = (
		sleep => 5
	);
	move_hash_values(\%rest_args,\%default_args,[keys %default_args]);
	while(!$self->node_pid_finish($node_idx,$node_pid_map)){
		print ".";
		sleep $default_args{sleep};
	}
	print "node $node_idx return\n";
}

# check whether the node is still running the given pids
sub node_pid_finish{
    my($self, $node_idx, $node_pid_map) = @_;
    my $ref = ref($node_pid_map);
    my @pids = ();
    if(!$ref){
	    push @pids, $node_pid_map;
    }elsif($ref eq "HASH"){
	    @pids = keys %$node_pid_map;
    }else{
	    @pids = @$node_pid_map;
    }
    my $more_alive = 0;
    foreach my $pid(keys %$node_pid_map){
	my $tmp_pid = $self->check_node_pid($node_idx,$pid, echo => 0);
        $more_alive = $more_alive || $tmp_pid;
    }
    return !$more_alive;
}



sub check_node_pname{
    my($self,$node_idx, $pname, @rest_args) = @_;
    my $cmd = "ps ax|grep -P \"$pname\"";
    my $output = $self->execute_on_node($node_idx,$cmd,@rest_args);
    if($output){
	    if($output =~ m/^\s*(\d+)/){
		    return {$1 => 1};
	    }
    }
    return undef;
}

sub check_node_pid{
    my($self,$node_idx, $pid,@rest_args) = @_;
    my $cmd = "ps cax|grep -o -P '$pid'";
    return $self->execute_on_node($node_idx,$cmd,@rest_args);
}


sub dump{
    my $self = shift;
    print Dumper($self);
}

sub _execute_cmd{
    my $cmd = shift;
    my %default_args = (
        echo => 1,
        message => undef
    );
    my %rest_args = (@_);
    move_hash_values(\%rest_args,\%default_args,[qw(echo message)]);
        $default_args{echo} and print "[info] $cmd\n";
        $default_args{message} and print "[info] $default_args{message}\n";
        my $output = `$cmd`;
        chomp $output;
        return $output;
    }
