#!/usr/bin/env perl

use strict;
use warnings;
use File::Basename;
use Getopt::Std;
use XML::LibXML;
use MBmaint::DSmeta;

my $databaseName;
my $datasetId;
my $dsMeta;
my $xmlStr;

# print help info and exit
our $opt_h; 
our $opt_l;
our $opt_v; 

sub usage {
    print basename($0) . " - export XML from Metabase for maintenance tasks\n\n";
    print "Usage: \n";
    print basename($0) . " [options] <task name> <dataset id>\n\n";
    print "Options: \n";
    print "-h \tprint help information.\n";
    print "-l \tlist available dataset metadata types (templates) and exit.\n";
    print "-v \trun in verbose mode.\n";
}

# Get command line options
getopts('hlv');

$opt_h = 0,  if (not defined $opt_h);
$opt_l = 0,  if (not defined $opt_l);
$opt_v = 0,  if (not defined $opt_v);

if ($opt_h) {
    usage();
    exit;
}

# List the available task names and exit
if ($opt_l) {
    print "Available task names: \n";
    $dsMeta = MBmaint::DSmeta->new();
    $dsMeta->listTemplates();
    exit;
}

# If no command line arguments are passed in, then print usage info and exit
if ($#ARGV <= 0) {
    usage();
    exit;
}

my $dsType = $ARGV[0];
$datasetId = $ARGV[1];

# Create a dataset metadata object
$dsMeta = MBmaint::DSmeta->new();

# Export dataset metadata from metabase for the specified dataset type and dataset id
$xmlStr = $dsMeta->exportXML($dsType, $datasetId, $opt_v);

print STDOUT $xmlStr;
