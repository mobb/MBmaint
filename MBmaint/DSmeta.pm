package MBmaint::DSmeta;
use Moose;
use strict;
use warnings;
use Data::Dumper;
use MBmaint::DButil;
use Template;
use XML::LibXML;

# Attributes of the DSM object
has 'dataset'       => ( is => 'rw', isa => 'HashRef' );
has 'mb'            => ( is => 'rw', isa => 'Object' );
has 'verbose'       => ( is => 'rw', isa => 'Int' );

my $configFilename = "./config/MBmaint.ini";
my $TEMPLATE_DIR = "./templates/";

sub BUILD {
    my $self = shift;

    $self->mb(MBmaint::DButil->new({configFile => $configFilename, verbose => $self->verbose }));
}

sub DEMOLISH {
    my $self = shift;
}

sub loadXML {

    # Populate the DSMeta data structure from an XML document.

    my $self = shift;
    my $dataFilename = shift;
    my $verbose = shift;

    my $attr;
    my $attrName;
    my $attributeRef;
    my $dom;
    my $href;
    my @tableNodes;
    my @rowNodes;
    my @childNodes;
    my $dataset = {};
    my $sth;

    my $firstRow; 
    my @colNames = (); 
    my @colValues = ();
    my $fn;
    my $nodemap;
    my $node;
    my @tmpArr;
    my %tableKeys;
    #my @keyColumns;
    my $keyColumnsRef;
    my $tableName;

    # Internal datastructure example. The 'names' list is the names
    # of the database columns. The 'values' list contains the database
    # values, in the same order as the 'names'.
    #
    # $dataset = {
    #          'DatasetPersonnel' => {
    #                            'names' => [
    #                                           'DataSetID',
    #                                           'NameID',
    #                                           'AuthorshipOrder',
    #                                           'AuthorshipRole'
    #                                       ],
    #                            'values' => [
    #                                          [
    #                                            '10',
    #                                            'sbclter',
    #                                            '1',
    #                                            'creator'
    #                                          ],
    #                                          [
    #                                            '10',
    #                                            'lwashburn',
    #                                            '2',
    #                                            'creator'
    #                                          ]
    #                                        ]
    #                          }
    #    };
    
    # sample XML data file:
    # <MB_content task="tsud" datasetid="10">
    #  <table name="DatasetPersonnel">
    #    <row>
    #      <column name="DataSetID">10</column>
    #      <column name="NameID">sbclter</column>
    #      <column name="AuthorshipOrder">1</column>
    #      <column name="AuthorshipRole">creator</column>
    #    </row>
    #    <row>
    #      <column name="DataSetID">10</column>
    #      <column name="NameID>lwashburn"</column>
    #      <column name="AuthorshipOrder">2</column>
    #      <column name="AuthorshipRole">creator</column>
    #    </row>
    #  </table>
    # </MB_content>

    # Create a DOM from the XML document that contains the data to send to Metabase 
    print "Reading XML file: " . $dataFilename . "\n", if $verbose;
    $dom = XML::LibXML->load_xml(location => $dataFilename, { no_blanks => 1 });

    # Find <table> entries. There may be data for multiple tables.
    @tableNodes = $dom->findnodes("/metabase2_content/table");

    # Loop through each table in the input XML
    for my $n (@tableNodes) {
        $firstRow = 1;
        # attributes of the XML element "table"
        $tableName = $n->getAttribute("name");
        # Top level hash element of internal data structure is the name of the table, i.e. "DataSetPersonnel"
        $dataset->{$tableName} = {};

        $keyColumnsRef = $self->mb->getKeyColumns($tableName);
        #DBI::dump_results($href);

        @{$dataset->{$tableName}{'keyColumns'}} = @$keyColumnsRef;
    
        # Get the row elements 
        @rowNodes = $n->getChildrenByTagName("row");
        # Loop through rows
        for my $r (@rowNodes) {
            # Get the field names
            @childNodes = $r->getChildrenByTagName("column");
            # Loop through fields (columns)
            # The <column> elements can have an optional 'src="file"' attribute. If this is
            # present, then the text value of this element is the name of a file to read, where
            # the contents of the file will be used as the text value, for example:
            #     <column name="MethodStep_xml" src="file">ds10_methods.xml</column>
            for my $c (@childNodes) {
                # If this is the first row that we have processed, then save the column name
                if ($firstRow) {
                    push(@colNames, $c->getAttribute("name"));
                }

                # The column value can come from the XML text field of the input XML file, or if
                # the attribute "src=<file>" is set, then the value will be the entire contents
                # of the filename that is in the XML text field, i.e.
                #
                #      <column name="MethodStep_xml" src="file">ds10_methods.xml</column>
                my $srcType = $c->getAttribute("src");
                if (not defined $srcType) {
                    push(@colValues, $c->textContent);
                }
                elsif (lc($srcType) eq "file") {
                    my $fn = $c->textContent; 
                    my $fh; 
                    open($fh, '<', $fn) or die "Can't open file: " . $fn . "\n";
                    my $content = join('', <$fh>);   
                    push(@colValues, $content);
                    close($fh);
                } elsif ($srcType ne "") {
                    die "unknown src type file: " . $dataFilename . ", element: " . $c->getName();
                }
            }

            # Record database column names and values to our internal data structure. Only record the names once, i.e. for the
            # first row.
            if ($firstRow) {
                @{$dataset->{$tableName}{'names'}} = @colNames;

                push(@{$dataset->{$tableName}{'values'}}, [ @colValues ]);

                $firstRow = 0;
            } else {
                push(@{$dataset->{$tableName}{'values'}},  [ @colValues ]);
            }

            @colNames = (); 
            @colValues = ();
        }
    }

    #print Dumper($dataset);
    $self->dataset($dataset);
}

sub sendToDB {
    my $self = shift;
    my $verbose = shift;

    my $href;
    my @keyColumns;
    my @names;
    my @valueRows;
    my @values;
    my $v;

    # Top level keys are the table names.
    my @tables = keys(%{$self->dataset});
    for my $table (@tables) {
        #print "table: " . "$table" . "\n";

        $href = $self->dataset->{$table};
        @keyColumns = @{$href->{'keyColumns'}};

        @names = @{$href->{'names'}};
        @valueRows = @{$href->{'values'}};

        my $i;
        # Loop through the internal representation of the data set metadata and send one row
        # of data to metabase at a time.
        for ($i = 0 ; $i < scalar(@valueRows); $i++) {
            @values = @{$valueRows[$i]};
            $self->mb->sendRow($table, \@keyColumns, \@names, \@values, $verbose);
        }
    }
   
    # Commit the transaction, close the database.
    $self->mb->closeDB($verbose);

    return;
}

sub listTemplates {
    my $self = shift;

    opendir(DIR, $TEMPLATE_DIR) or die "Can't open $TEMPLATE_DIR";
    my @files = sort (grep(/query_pg_/, readdir(DIR)));

    for my $f (@files) {
        $f =~ s/query_pg_//;
        $f =~ s/.tt//;
        print $f . "\n";
    }
}

sub exportXML {

    my $self = shift;
    my $taskName = shift;
    my $datasetId = shift;
    my $verbose = shift;

    my $doc;
    my $output;
    my $taskXML;
    my $templateName;
    my %templateVars;

    # Template files in the ./templates directory have the format
    # 'query_pg_'<task name>'.tt', for example:
    # 
    #     query_pg_DataSetMethods.tt
    #
    $templateName = $TEMPLATE_DIR . 'query_pg_' . $taskName . ".tt";
    $templateVars{'datasetid'} = $datasetId;

    my $tt = Template->new({ RELATIVE => 1, ABSOLUTE => 1});

    # Fill in the template, send template output to a text string
    $tt->process($templateName, \%templateVars, \$output )
        || die $tt->error;

    #print $output . "\n";

    $taskXML = $self->mb->submitSQL($output, $verbose);

    eval {
        $doc = XML::LibXML->load_xml(string => $taskXML, { no_blanks => 1 });
    };

    if ($@) {
        print STDERR "Error processing task XML: $@\n";
        print STDERR "The following is the invalid XML that was returned from Metabase: \n";
        print STDERR $taskXML. "\n";
        die ": Processing halted because the generated XML document is not valid.\n";
    }

    return $doc->toString(1);
}

# Make this Moose class immutable
__PACKAGE__->meta->make_immutable;

1;

__END__
