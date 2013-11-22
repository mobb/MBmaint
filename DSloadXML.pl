#!/usr/bin/env perl

use strict;
use warnings;
use File::Basename;
use Getopt::Std;
use XML::LibXML;
use MBmaint::DSmeta;

my $databaseName;
my $datasetId;

# print help info and exit
our $opt_h; 
our $opt_v; 

my $dsm;

sub usage {
    print basename($0) . " - ingest dataset metadata into Metabase\n\n";
    print "Usage: \n";
    print basename($0) . " [options] datafile\n\n";
    print "Options: \n";
    print "-h \tprint help information.\n";
    print "-v \trun in verbose mode.\n";
}

# Get command line options
getopts('hv');

$opt_h = 0,  if (not defined $opt_h);
$opt_v = 0,  if (not defined $opt_v);

if ($opt_h) {
    usage();
    exit;
}

# If no command line arguments are passed in, then print usage info and exit
if ($#ARGV == -1) {
    usage();
    exit;
}

my $filename = $ARGV[0];
# Create a dataset metadata object
$dsm = MBmaint::DSmeta->new();

# Load an XML file to populate a dataset metadata object
$dsm->loadXML($filename, $opt_v);

# Send the dataset metadata to Metabase
$dsm->sendToDB($opt_v);
