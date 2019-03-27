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
my($configFileName) = @ARGV;

# Read in config file
my $configFile 	= Config::General->new($configFileName);
my %config 		= $configFile->getall;

# print Dumper(\%config);
# exit;

my %variables 			= %{ $config{variables}{variable} };

my %experiments			= %{ $config{experiment} };
my @experimentNames 	= keys %experiments;
my @starts 				= map{ $experiments{$_}{start}; } (@experimentNames);
my @ends 				= map{ $experiments{$_}{end}; } (@experimentNames);
my @frequenciesHr		= map{ $experiments{$_}{frequencyHr}; } (@experimentNames);
my @ensemblePaths		= map{ $experiments{$_}{path}; } (@experimentNames);
my @ensGridTypes		= map{ $experiments{$_}{gridType}; } (@experimentNames);

my $startLeadTimeHr		= $config{leadTime}{startLeadTimeHr};
my $endLeadTimeHr		= $config{leadTime}{endLeadTimeHr};
my $leadTimeIncrHr		= $config{leadTime}{leadTimeIncrHr};

my $verificationPath	= $config{verification}{path};
my $verifType 			= $config{verification}{type};
my $verifGrid			= $config{verification}{grid};

my $useClimatology;
my $climatologyPath;
my $climatologyGrid;
if( exists $config{climatology} ) {
	$useClimatology 	= "True";
	$climatologyPath	= $config{climatology}{path};
	$climatologyGrid 	= $config{climatology}{grid};
} else {
	$useClimatology 	= "False";
	$climatologyPath	= "null";
	$climatologyGrid 	= "null";
}

my $plotPath			= $config{output}{path};
my $removeAttFiles 		= $config{output}{removeAttFiles};

my $useQueue 			= $config{pbs}{useQueue} eq "True";
my $PBSaccount			= $config{pbs}{account};
my $PBSqueue			= $config{pbs}{queueName} // "batch";
my $PBSwalltime 		= $config{pbs}{walltime};
my $PBScpu				= $config{pbs}{cpu};
my $PBSlogDir 			= $config{pbs}{logDir};
my $PBSvmem				= $config{pbs}{vmem} // "20GB";

# Calculate the lead times to be used
my $nLeads = ($endLeadTimeHr - $startLeadTimeHr) / $leadTimeIncrHr + 1;
my @leadTimesHr = map{  $_ * $leadTimeIncrHr + $startLeadTimeHr; } (0 .. $nLeads-1);

# Combine experiment paramaters into comma-seperated strings
my $experimentNames 	= join( ",", @experimentNames );
my $starts 				= join( ",", @starts );
my $ends 				= join( ",", @ends );
my $frequenciesHr		= join( ",", @frequenciesHr );
my $ensemblePaths		= join( ",", @ensemblePaths );
my $ensGridTypes		= join( ",", @ensGridTypes );

# Create output directories if necessary
foreach my $experimentName (@experimentNames) {
	foreach my $variableName (keys %variables) {
		mkpath("$plotPath/$experimentName/$variableName/") unless -e "$plotPath/$experimentName/$variableName/";
	}
}
mkpath("$PBSlogDir") unless -e "$PBSlogDir";

# Create a perl script for each lead time
foreach my $leadTimeHr (@leadTimesHr) {
	my $fff = sprintf("%03s",$leadTimeHr);

	my $qsubFileName 	= "$plotPath/plotMap_f${fff}.pl";

	open( my $qsubFile, ">$qsubFileName" ) || die "Couldn't open file $qsubFileName";

	# Build new perl script to submit to queue
	print $qsubFile "#! /usr/bin/perl -W\n";

	if($useQueue) {
		system( "rm $PBSlogDir/plotMap_f${fff}.out" ) unless !-e "$PBSlogDir/plotMap_f${fff}.out";
		my $header = qq{
			#
			# PBS Directives
			#
			#PBS -A $PBSaccount
			#PBS -q $PBSqueue
			#PBS -l nodes=1:ppn=$PBScpu
			#PBS -l walltime=$PBSwalltime
			#PBS -l vmem=$PBSvmem
			#PBS -N plotMap_f${fff}
			#PBS -o $PBSlogDir/plotMap_f${fff}.out
			#PBS -j oe

			use strict;
			use warnings;
			use threads;
			use Thread::Queue;
			chdir("$cwd");

			my \$leadTimeHr="$leadTimeHr";
			my \$nJobs="$PBScpu";
			my \$queue = new Thread::Queue;
		};
		$header =~ s/^\t+//gm;
		print $qsubFile $header;
	}

	my @cleanup;

	# Create an NCL input file for each lead/variable combination
	foreach my $variableName (keys %variables) {
		# Get the hash for this variable
		my %variable = %{ $variables{$variableName} };
		my $variableNiceName 	= $variable{niceName};
		my $verificationName 	= $variable{verifName};
		my $grib2Name = $variable{grib2Name};
		my $climatologyName;
		if( $useClimatology eq "True" ) {
			$climatologyName = $variable{climoName};
		} else {
			$climatologyName = "null";
		}

		# Determine file names
		my $nclFileName 			= "$plotPath/$variableName\_f$leadTimeHr.inp";

		my $spreadMapAttFilename 	= "$plotPath/$variableName\_f$leadTimeHr\_spread_map.inp";
		my $spreadZonalAttFilename 	= "$plotPath/$variableName\_f$leadTimeHr\_spread_zonal.inp";

		my $rmseMapAttFilename 		= "$plotPath/$variableName\_f$leadTimeHr\_rmse_map.inp";
		my $rmseZonalAttFilename 	= "$plotPath/$variableName\_f$leadTimeHr\_rmse_zonal.inp";
		
		my $ratioMapAttFilename 	= "$plotPath/$variableName\_f$leadTimeHr\_ratio_map.inp";
		my $ratioZonalAttFilename 	= "$plotPath/$variableName\_f$leadTimeHr\_ratio_zonal.inp";
		
		my $biasMapAttFilename 		= "$plotPath/$variableName\_f$leadTimeHr\_bias_map.inp";
		my $biasZonalAttFilename 	= "$plotPath/$variableName\_f$leadTimeHr\_bias_zonal.inp";

		my $outlierMapAttFilename 	= "$plotPath/$variableName\_f$leadTimeHr\_outlier_map.inp";
		my $outlierZonalAttFilename = "$plotPath/$variableName\_f$leadTimeHr\_outlier_zonal.inp";

		my $spreadSkillAttFilename 		= "$plotPath/$variableName\_f$leadTimeHr\_spreadSkill.inp";
		my $normSpreadSkillAttFilename 	= "$plotPath/$variableName\_f$leadTimeHr\_normSpreadSkill.inp";
		
		# Write attribute files for each type of plot
		writeAttFile($spreadMapAttFilename, $variableName, "error", "map", $config{plots});
		writeAttFile($spreadZonalAttFilename, $variableName, "error", "zonal", $config{plots});

		writeAttFile($rmseMapAttFilename, $variableName, "error", "map", $config{plots});
		writeAttFile($rmseZonalAttFilename, $variableName, "error", "zonal", $config{plots});

		writeAttFile($ratioMapAttFilename, $variableName, "ratio", "map", $config{plots});
		writeAttFile($ratioZonalAttFilename, $variableName, "ratio", "zonal", $config{plots});

		writeAttFile($biasMapAttFilename, $variableName, "bias", "map", $config{plots});
		writeAttFile($biasZonalAttFilename, $variableName, "bias", "zonal", $config{plots});

		writeAttFile($outlierMapAttFilename, $variableName, "outlier", "map", $config{plots});
		writeAttFile($outlierZonalAttFilename, $variableName, "outlier", "zonal", $config{plots});

		writeAttFile($spreadSkillAttFilename, $variableName, "sreliability", "map", $config{plots});
		writeAttFile($normSpreadSkillAttFilename, $variableName, "sreliability", "zonal", $config{plots});

		push(@cleanup, $nclFileName);
		push(@cleanup, $spreadMapAttFilename);
		push(@cleanup, $spreadZonalAttFilename);
		push(@cleanup, $rmseMapAttFilename);
		push(@cleanup, $rmseZonalAttFilename);
		push(@cleanup, $ratioMapAttFilename);
		push(@cleanup, $ratioZonalAttFilename);
		push(@cleanup, $biasMapAttFilename);
		push(@cleanup, $biasZonalAttFilename);
		push(@cleanup, $outlierMapAttFilename);
		push(@cleanup, $outlierZonalAttFilename);
		push(@cleanup, $spreadSkillAttFilename);
		push(@cleanup, $normSpreadSkillAttFilename);

		# Create the NCL input file
		open( my $file, ">$nclFileName" ) || die("Couldn't create NCL input file $nclFileName!\n");
		
		print $file "$ensemblePaths\n";
		print $file "$ensGridTypes\n";
		print $file "$experimentNames\n";
		print $file "$starts\n";
		print $file "$ends\n";
		print $file "$frequenciesHr\n";
		print $file "$leadTimeHr\n";

		print $file "$variableName\n";
		print $file "$grib2Name\n";
		print $file "$variableNiceName\n";
		
		print $file "$verificationPath\n";
		print $file "$verificationName\n";
		print $file "$verifType\n";
		print $file "$verifGrid\n";

		print $file "$useClimatology\n";
		print $file "$climatologyPath\n";
		print $file "$climatologyName\n";
		print $file "$climatologyGrid\n";

		print $file "$plotPath\n";

		print $file "$spreadMapAttFilename\n";
		print $file "$spreadZonalAttFilename\n";
		print $file "$rmseMapAttFilename\n";
		print $file "$rmseZonalAttFilename\n";
		print $file "$ratioMapAttFilename\n";
		print $file "$ratioZonalAttFilename\n";
		print $file "$biasMapAttFilename\n";
		print $file "$biasZonalAttFilename\n";
		print $file "$outlierMapAttFilename\n";
		print $file "$outlierZonalAttFilename\n";
		print $file "$spreadSkillAttFilename\n";
		print $file "$normSpreadSkillAttFilename\n";
		print $file "$removeAttFiles";
		close($file);

		# Write command to run NCL with this input file into the perl script
		if($useQueue) {
			print $qsubFile qq(\$queue->enqueue("$nclFileName");\n);
		} else {
			print $qsubFile qq(print STDOUT "Starting $nclFileName\n";\n);
			print $qsubFile qq(system("ncl plotMap.ncl \\'inputFilename=\\"$nclFileName\\"\\'");\n);
			print $qsubFile qq(print STDOUT "Done with $nclFileName\n";\n);
		}

	}

	my $all_cleanup = join(@cleanup, ",");

	if($useQueue) {
		print $qsubFile qq{
			my \@cleanup = split(q($all_cleanup), ",");
		};

		print $qsubFile q{
			my @threadPool = map{threads->create("runScript")} 1..$nJobs;
			$_->join for @threadPool;

			foreach my $filename (@cleanup) {
				unlink($filename);
			}

			print STDOUT "Completed f$leadTimeHr\\n\\n";

			exit;

			sub runScript {
				while(my $input = $queue->dequeue_nb() ) {
					print STDOUT "Starting $input\n";
					system("ncl plotMap.ncl \\'inputFilename=\\"$input\\"\\'");
					print STDOUT "Done with $input\n";
				}
			}
		};
	}

	close($qsubFile);

	# If we are using PBS scheduling, submit the job; otherwise run it here
	if($opt_t) {
		print STDOUT qq( Test file $qsubFileName created.\n);
	} else {
		if($useQueue) {
			print STDOUT "Submitting for f$leadTimeHr: \n";
			system("qsub $qsubFileName");
		} else {
			print STDOUT qq(Running $qsubFileName\n);
			do $qsubFileName;
		}
		# Once submitted/complete, we can clean-up the perl script
		system("rm $qsubFileName");
	}
}

exit;

# Write a file with plot attributes we wish to override
sub writeAttFile {
	my($filename, $variableName, $datatype, $plottype, $hashRef) = @_;
	# my %attHash = %{ $hashRef };

	open(my $file, ">$filename") || die ("Couldn't open attribute file $filename!\n");
	processAtts($file, $variableName, $datatype, $plottype, $hashRef);
	close($file);
}

sub processAtts {
	my($file, $variableName, $datatype, $plottype, $hashRef) = @_;
	my %attHash = %{ $hashRef };	
	my @validMarkers = ("variable", "datatype", "plottype" );
	
	foreach my $attKey (keys %attHash) {
		if( $attKey ~~ @validMarkers ) {
			my $match;
			switch($attKey) {
				case "variable" { $match = $variableName }
				case "plottype" { $match = $plottype }
				case "datatype" { $match = $datatype }
			}
			my %subhash = %{ $attHash{$attKey} };
			foreach my $id (keys %subhash) {
				if( $id eq $match ) {
					processAtts($file, $variableName, $datatype, $plottype, $subhash{$id});
				}
			}
		} else {
			print $file "$attKey=$attHash{$attKey}\n";
		}
	}
}