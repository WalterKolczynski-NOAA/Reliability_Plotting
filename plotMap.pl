#! /usr/bin/perl -W

#
# Module imports
#
use lib "/scratch4/NCEPDEV/ensemble/save/Walter.Kolczynski/PerlModules";
use strict;
use warnings;
use Getopt::Std;
use vars qw($opt_h $opt_t);
use Time::Local;
use File::Copy;
use File::Path;
use Env;
use Cwd;
use threads;
use Thread::Queue;
use Data::Dumper;
use Config::General;
use Switch;

getopts('ht');

my $cwd = getcwd();

# Use command line argument as the configuration file name
my($config_filename) = @ARGV;

# Read in config file
my $config_file_handle 	= Config::General->new($config_filename);
my %config 				= $config_file_handle->getall;

# print Dumper(\%config);
# exit;

my %variables 			= %{ $config{variables}{variable} };

my %experiments			= %{ $config{experiments}{experiment} };
my @experiment_names 	= keys %experiments;
my @starts 				= map{ $experiments{$_}{start}; } (@experiment_names);
my @ends 				= map{ $experiments{$_}{end}; } (@experiment_names);
my @frequency_hr		= map{ $experiments{$_}{frequency_fr}; } (@experiment_names);
my @ensemble_paths		= map{ $experiments{$_}{path}; } (@experiment_names);
my @ensemble_grid_types	= map{ $experiments{$_}{grid_type}; } (@experiment_names);

my $start_lead_hr		= $config{lead_time}{start_lead_hr};
my $end_lead_hr			= $config{lead_time}{end_lead_hr};
my $lead_time_stride_hr	= $config{lead_time}{lead_time_stride_hr};

my $verification_path	= $config{verification}{path};
my $verification_type 	= $config{verification}{type};
my $verification_grid	= $config{verification}{grid};

my $use_climatology;
my $climatology_path;
my $climatology_grid;
if( exists $config{climatology} ) {
	$use_climatology 	= "True";
	$climatology_path	= $config{climatology}{path};
	$climatology_grid 	= $config{climatology}{grid};
} else {
	$use_climatology 	= "False";
	$climatology_path	= "null";
	$climatology_grid 	= "null";
}

my $output_path				= $config{output}{path};
my $log_dir 				= $config{output}{log_dir};
my $remove_attribute_files 	= $config{output}{remove_attribute_files};

my $use_queue 			= $config{scheduler}{use_queue} eq "True";
my $slurm_account		= $config{scheduler}{account};
my $slurm_partition		= $config{scheduler}{partition} // "theia";
my $slurm_qos			= $config{scheduler}{qos} // "batch";
my $slurm_walltime 		= $config{scheduler}{walltime};
my $slurm_n_nodes		= $config{scheduler}{n_nodes} // 1;
my $slurm_n_cpus		= $config{scheduler}{n_cpus} // 1;
my $slurm_memory		= $config{scheduler}{memory} // "20GB";

# Calculate the lead times to be used
my $n_leads = ($end_lead_hr - $start_lead_hr) / $lead_time_stride_hr + 1;
my @lead_times_hr = map{  $_ * $lead_time_stride_hr + $start_lead_hr; } (0 .. $n_leads-1);

# Combine experiment paramaters into comma-seperated strings
my $experiment_names 	= join( ",", @experiment_names );
my $starts 				= join( ",", @starts );
my $ends 				= join( ",", @ends );
my $frequency_hr		= join( ",", @frequency_hr );
my $ensemble_paths		= join( ",", @ensemble_paths );
my $ensemble_grid_types		= join( ",", @ensemble_grid_types );

# Create output directories if necessary
foreach my $experiment_name (@experiment_names) {
	foreach my $variable_name (keys %variables) {
		mkpath("$output_path/$experiment_name/$variable_name/") unless -e "$output_path/$experiment_name/$variable_name/";
	}
}
mkpath("$log_dir") unless -e "$log_dir";

# Create a perl script for each lead time
foreach my $lead_time_hr (@lead_times_hr) {
	my $fff = sprintf("%03s",$lead_time_hr);

	my $slurm_filename 	= "$output_path/plotMap_f${fff}.pl";

	open( my $slurm_file, ">$slurm_filename" ) || die "Couldn't open file $slurm_filename";

	# Build new perl script to submit to queue
	print $slurm_file "#! /usr/bin/perl -W\n";

	if($use_queue) {
		system( "rm $log_dir/plotMap_f${fff}.out" ) unless !-e "$log_dir/plotMap_f${fff}.out";
		my $header = qq{
			#
			# Slurm Directives
			#
			#SBATCH --account $slurm_account
			#SBATCH --partition $slurm_partition
			#SBATCH --qos $slurm_qos
			#SBATCH --nodes $slurm_n_nodes
			#SBATCH --ntasks $slurm_n_cpus
			#SBATCH --time $slurm_walltime
			#SBATCH --mem $slurm_memory
			#SBATCH --job-name plotMap_f${fff}
			#SBATCH --output $log_dir/plotMap_f${fff}.out

			use strict;
			use warnings;
			use threads;
			use Thread::Queue;
			chdir("$cwd");

			my \$lead_time_hr = "$lead_time_hr";
			my \$fff = "$fff";
			my \$n_jobs = "$slurm_n_cpus";
			my \$queue = new Thread::Queue;
		};
		$header =~ s/^\t+//gm;
		print $slurm_file $header;
	}

	my @cleanup;

	# Create an NCL input file for each lead/variable combination
	foreach my $variable_name (keys %variables) {
		# Get the hash for this variable
		my %variable = %{ $variables{$variable_name} };
		my $variable_nice_name 			= $variable{nice_name};
		my $variable_name_verification 	= $variable{verification_name};
		my $variable_name_ensemble 		= $variable{ensemble_name};
		my $variable_name_climatology;
		if( $use_climatology eq "True" ) {
			$variable_name_climatology 	= $variable{climatology_name};
		} else {
			$variable_name_climatology 	= "null";
		}

		# Determine file names
		my $ncl_filename = "$output_path/$variable_name\_f${fff}.inp";

		my $spread_map_att_filename 		= "$output_path/$variable_name\_f${fff}\_spread_map.inp";
		my $spread_zonal_att_filename 		= "$output_path/$variable_name\_f${fff}\_spread_zonal.inp";

		my $rmse_map_att_filename 			= "$output_path/$variable_name\_f${fff}\_rmse_map.inp";
		my $rmse_zonal_att_filename 		= "$output_path/$variable_name\_f${fff}\_rmse_zonal.inp";
		
		my $ratio_map_att_filename 			= "$output_path/$variable_name\_f${fff}\_ratio_map.inp";
		my $ratio_zonal_att_filename 		= "$output_path/$variable_name\_f${fff}\_ratio_zonal.inp";
		
		my $bias_map_att_filename 			= "$output_path/$variable_name\_f${fff}\_bias_map.inp";
		my $bias_zonal_att_filename 		= "$output_path/$variable_name\_f${fff}\_bias_zonal.inp";

		my $outlier_map_att_filename 		= "$output_path/$variable_name\_f${fff}\_outlier_map.inp";
		my $outlier_zonal_att_filename 		= "$output_path/$variable_name\_f${fff}\_outlier_zonal.inp";

		my $spread_skill_att_filename 		= "$output_path/$variable_name\_f${fff}\_spreadSkill.inp";
		my $norm_spread_skill_att_filename 	= "$output_path/$variable_name\_f${fff}\_normSpreadSkill.inp";
		
		# Write attribute files for each type of plot
		writeAttFile($spread_map_att_filename, $variable_name, "error", "map", $config{plots});
		writeAttFile($spread_zonal_att_filename, $variable_name, "error", "zonal", $config{plots});

		writeAttFile($rmse_map_att_filename, $variable_name, "error", "map", $config{plots});
		writeAttFile($rmse_zonal_att_filename, $variable_name, "error", "zonal", $config{plots});

		writeAttFile($ratio_map_att_filename, $variable_name, "ratio", "map", $config{plots});
		writeAttFile($ratio_zonal_att_filename, $variable_name, "ratio", "zonal", $config{plots});

		writeAttFile($bias_map_att_filename, $variable_name, "bias", "map", $config{plots});
		writeAttFile($bias_zonal_att_filename, $variable_name, "bias", "zonal", $config{plots});

		writeAttFile($outlier_map_att_filename, $variable_name, "outlier", "map", $config{plots});
		writeAttFile($outlier_zonal_att_filename, $variable_name, "outlier", "zonal", $config{plots});

		writeAttFile($spread_skill_att_filename, $variable_name, "sreliability", "map", $config{plots});
		writeAttFile($norm_spread_skill_att_filename, $variable_name, "sreliability", "zonal", $config{plots});

		push(@cleanup, $ncl_filename);
		push(@cleanup, $spread_map_att_filename);
		push(@cleanup, $spread_zonal_att_filename);
		push(@cleanup, $rmse_map_att_filename);
		push(@cleanup, $rmse_zonal_att_filename);
		push(@cleanup, $ratio_map_att_filename);
		push(@cleanup, $ratio_zonal_att_filename);
		push(@cleanup, $bias_map_att_filename);
		push(@cleanup, $bias_zonal_att_filename);
		push(@cleanup, $outlier_map_att_filename);
		push(@cleanup, $outlier_zonal_att_filename);
		push(@cleanup, $spread_skill_att_filename);
		push(@cleanup, $norm_spread_skill_att_filename);

		# Create the NCL input file
		open( my $file, ">$ncl_filename" ) || die("Couldn't create NCL input file $ncl_filename!\n");
		
		print $file "$ensemble_paths\n";
		print $file "$ensemble_grid_types\n";
		print $file "$experiment_names\n";
		print $file "$starts\n";
		print $file "$ends\n";
		print $file "$frequency_hr\n";
		print $file "$lead_time_hr\n";

		print $file "$variable_name\n";
		print $file "$variable_name_ensemble\n";
		print $file "$variable_nice_name\n";
		
		print $file "$verification_path\n";
		print $file "$variable_name_verification\n";
		print $file "$verification_type\n";
		print $file "$verification_grid\n";

		print $file "$use_climatology\n";
		print $file "$climatology_path\n";
		print $file "$variable_name_climatology\n";
		print $file "$climatology_grid\n";

		print $file "$output_path\n";

		print $file "$spread_map_att_filename\n";
		print $file "$spread_zonal_att_filename\n";
		print $file "$rmse_map_att_filename\n";
		print $file "$rmse_zonal_att_filename\n";
		print $file "$ratio_map_att_filename\n";
		print $file "$ratio_zonal_att_filename\n";
		print $file "$bias_map_att_filename\n";
		print $file "$bias_zonal_att_filename\n";
		print $file "$outlier_map_att_filename\n";
		print $file "$outlier_zonal_att_filename\n";
		print $file "$spread_skill_att_filename\n";
		print $file "$norm_spread_skill_att_filename\n";
		print $file "$remove_attribute_files";
		close($file);

		# Write command to run NCL with this input file into the perl script
		if($use_queue) {
			print $slurm_file qq(\$queue->enqueue("$ncl_filename");\n);
		} else {
			print $slurm_file qq(print STDOUT "Starting $ncl_filename\n";\n);
			print $slurm_file qq(system("ncl plotMap.ncl \\'inputFilename=\\"$ncl_filename\\"\\'");\n);
			print $slurm_file qq(print STDOUT "Done with $ncl_filename\n";\n);
		}

	}

	my $all_cleanup = join(@cleanup, ",");

	if($use_queue) {
		print $slurm_file qq{
			my \@cleanup = split(q($all_cleanup), ",");
		};

		print $slurm_file q{
			my @threadPool = map{threads->create("run_script")} 1..$n_jobs;
			$_->join for @threadPool;

			foreach my $filename (@cleanup) {
				print STDOUT "Deleting $filename";
				unlink($filename);
			}

			print STDOUT "Completed f$fff\\n\\n";

			exit;

			sub run_script {
				while(my $input = $queue->dequeue_nb() ) {
					print STDOUT "Starting $input\n";
					system("ncl plotMap.ncl \\'inputFilename=\\"$input\\"\\'");
					print STDOUT "Done with $input\n";
				}
			}
		};
	}

	close($slurm_file);

	# If we are using task scheduler, submit the job; otherwise run it here
	if($opt_t) {
		print STDOUT qq( Test file $slurm_filename created.\n);
	} else {
		if($use_queue) {
			print STDOUT "Submitting for f$fff: \n";
			system("sbatch $slurm_filename");
		} else {
			print STDOUT qq(Running $slurm_filename\n);
			do $slurm_filename;
		}
		# Once submitted/complete, we can clean-up the perl script
		system("rm $slurm_filename");
	}
}

exit;

# Write a file with plot attributes we wish to override
sub writeAttFile {
	my($filename, $variable_name, $datatype, $plottype, $hashRef) = @_;
	# my %attHash = %{ $hashRef };

	open(my $file, ">$filename") || die ("Couldn't open attribute file $filename!\n");
	processAtts($file, $variable_name, $datatype, $plottype, $hashRef);
	close($file);
}

sub processAtts {
	my($file, $variable_name, $datatype, $plottype, $hashRef) = @_;
	my %attHash = %{ $hashRef };	
	my @validMarkers = ("variable", "datatype", "plottype" );
	
	foreach my $attKey (keys %attHash) {
		if( $attKey ~~ @validMarkers ) {
			my $match;
			switch($attKey) {
				case "variable" { $match = $variable_name }
				case "plottype" { $match = $plottype }
				case "datatype" { $match = $datatype }
			}
			my %subhash = %{ $attHash{$attKey} };
			foreach my $id (keys %subhash) {
				if( $id eq $match ) {
					processAtts($file, $variable_name, $datatype, $plottype, $subhash{$id});
				}
			}
		} else {
			print $file "$attKey=$attHash{$attKey}\n";
		}
	}
}