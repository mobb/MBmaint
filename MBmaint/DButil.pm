package MBmaint::DButil;
use Moose;
use strict;
use warnings;
use Config::Simple;
use Data::Dumper;
use DateTime;
use DBI;
use List::MoreUtils qw( each_array );

# This module contains methods that perform database tasks, currently with
# the PostgreSQL implementation of the Metabase2 schema.

# Attributes 
has 'configFile'        => ( is => 'rw', isa => 'Str', required => 1);
has 'dbhtxn'            => ( is => 'rw', isa => 'Object');
has 'dbh'               => ( is => 'rw', isa => 'Object');
has 'stmtCount'         => ( is => 'rw', isa => 'Int');
has 'rowsDeletedCount'  => ( is => 'rw', isa => 'Int');
has 'rowsUpdatedCount'  => ( is => 'rw', isa => 'Int');
has 'rowsInsertedCount' => ( is => 'rw', isa => 'Int');
has 'rowsSkippedCount'  => ( is => 'rw', isa => 'Int');
has 'schemaName'        => ( is => 'rw', isa => 'Str');

# An update action should delete only one row. If the rows affected is greater than this
# then something went very wrong (internal error) and a transaction rollback should occur.
my $MAX_ROWS_ALLOWED_PER_DELETE = 1;
my $DELETE_ACTION_TYPE = "delete";
my $UPDATE_ACTION_TYPE = "update";

# Note: Can't override new() with Moose, so use 'BUILD' which is like a new() postprocessing, i.e.
# 'BUILD' is called after the object is created.
sub BUILD {
    my $self = shift;

    $self->schemaName("metabase2");

    $self->stmtCount(0);
    # Load config file
    my $cfg = new Config::Simple($self->configFile);

    if (not defined $cfg) {
        die "Error: configuration file \"$self->configFile\" not found.\n";
    }

    # search PostgreSQL account and pass
    my $account = $cfg->param('account');
    my $pass = $cfg->param('pass');
    my $host = $cfg->param('host');
    my $port = $cfg->param('port');
    my $dbName = $cfg->param('database');

    # Establish two database connections, one for transaction processing and one for querying during the transaction.
    # 
    # Connection for transaction processing
    # (Because 'RaiseError' is set to true, any db error or Perl error will cause a transaction rollback.)
    my $dbi =  'dbi:Pg:dbname=' . $dbName . ';host=' . $host . ' ;port=' . $port;
    $self->dbhtxn(DBI->connect($dbi, $account, $pass, { AutoCommit => 0, RaiseError => 1, pg_server_prepare => 1}));
    my $rv = $self->dbhtxn->do('set search_path to metabase2, public');

    # Connection for reading
    # AutoCommit = 1, so transactions are disabled.
    $self->dbh(DBI->connect($dbi, $account, $pass, { AutoCommit => 1, RaiseError => 1, pg_server_prepare => 1}));
    $rv = $self->dbh->do('set search_path to ' . $self->schemaName . ', public');

    # This essentially begins a transaction. Changes won't be made permanent until $dbh->commit is called.
    #$self->dbhtxn->{AutoCommit} = 0; 
    #$self->dbhtxn->{RaiseError} = 1;

    # Keep track of sql inserts and updates sent to the database, and the input rows skipped due to the
    # values of the input row and row in the database having the same values.
    $self->rowsDeletedCount(0);
    $self->rowsInsertedCount(0);
    $self->rowsUpdatedCount(0);
    $self->rowsSkippedCount(0);
}

sub DEMOLISH {
    my $self = shift;

    $self->dbhtxn->disconnect;
    $self->dbh->disconnect;
}

sub sendRow {

    # Send one row of information to the database. Performing an SQL update is the default
    # action, but if a row already exists with the specified primary key fields, an insert
    # is performed instead. Also, the operation type can be specified for each row. See the
    # DSmeta::loadXML method for more details.

    my $self = shift;
    my $tableName = shift;
    my $keyColumnsRef = shift;
    my $colNamesRef = shift;
    my $colValuesRef = shift;
    my $action = shift;
    my $verbose = shift;

    my $colName;
    my @colNamesFromDB = ();
    my $colType;
    my @colTypesFromDB = ();
    my $doDelete = 0;
    my $doInsert = 0;
    my $doUpdate = 0;
    my %DBtypes = ();
    my %DBvalues = ();
    my $inputRow = "";
    my %inputValues = ();
    my $rc;
    my $sthTxn;
    my $sthRead;
    my @values = [];

    # Check if row exists by checking for a row matching values in the key columns
    $sthRead = $self->genSelectStmt($tableName, $keyColumnsRef, $colNamesRef, $colValuesRef);
    $rc = $sthRead->execute;

    # Fetch a row from the database
    @values = $sthRead->fetchrow_array;

    # Get the column names for this table from the database
    my $sth = $self->dbh->column_info( undef, $self->schemaName, $tableName, undef);
    for my $rel (@{$sth->fetchall_arrayref({})}) {
        $colName = $rel->{COLUMN_NAME};
        $colName =~ s/^"//;
        $colName =~ s/"$//;
        #print "colname: " . $colName . "\n";
        push(@colNamesFromDB, $colName);

        # This variable is unique to the DBD:pg driver. It requests that the PostgreSQL driver
        # return the PostgreSQL native data types for the columns.
        $colType = $rel->{pg_type};
        #print "column type: " . $colType . "\n";
        push(@colTypesFromDB, $colType);
    }

    #print "Dumper(values): " . @values . "\n";

    if (lc($action) eq $DELETE_ACTION_TYPE) {
        print "delete action\n";
        # If a row exists in the database already (values from the select were returned), update it (or delete if requested). 
        if (@values > 0) {
            print "do delete\n";
            # A row exists and "delete" has been requested.
            $sthTxn = $self->genDeleteStmt($tableName, $keyColumnsRef, $colNamesRef, $colValuesRef);
            $self->rowsDeletedCount($self->rowsDeletedCount + 1);
            $doDelete = 1;
        } else {
            print "no row to delete\n";
            print STDERR "Delete requested but requested row does not exist.\n";

            if ($self->stmtCount > 0) {
                print STDERR "Rolling back transaction.\n";
                $self->dbhtxn->rollback;
            }

            die "Exiting...\n";
        }
    } elsif (lc($action) eq $UPDATE_ACTION_TYPE) {
        if (@values > 0) {
            # Update is the default action, but we still have to determine if an update is necessary by
            # comparing all values in the input row with all values in the target row in the database.

            # Build hashes for the column name/values from the database and one from the
            # input, for easy comparison between the two.

            # Build a hash of input name/values
            my $inputIter = each_array( @$colNamesRef, @$colValuesRef);
            my $icnt = 0;
            while ( my ($name, $value) = $inputIter->() ) {
                # Remove leading and trailing quotes
                $name =~ s/^"//;
                $name =~ s/"$//;
                $inputValues{$name} = $value;
                #print "name: " . $name . "\n";
                #print "input value: " . $inputValues{$name} . "\n";
                # Construct a string of the values for logging
                my $sep = $icnt == 0 ? "" : ", ";
                $inputRow .= "$sep$name=$value";
                $icnt++;
            }
    
            #print "names: " . Dumper(@colNamesFromDB) . "\n";
            #print "values: " . Dumper(@values) . "\n";
            # Build a hash of DB name/values
            my $DBiter = each_array( @colNamesFromDB, @values);
            while ( my ($name, $value) = $DBiter->()) {
                $value = "", if (not defined $value);
                $name =~ s/^"//;
                $name =~ s/"$//;
                $DBvalues{$name} = "";
                $DBvalues{$name} = $value;
                #print "db name: " . $name . "\n";
                #print "db value: " . $DBvalues{$name} . "\n";
            }
      
            # Build a hash of column name => column type
            my $typeIter = each_array( @colNamesFromDB, @colTypesFromDB);
            while ( my ($name, $type) = $typeIter->() ) {
                # Remove leading and trailing quotes
                $name =~ s/^"//;
                $name =~ s/"$//;
                $DBtypes{$name} = $type;
                #print "name: " . $name . "\n";
                #print "type: " . $DBtypes{$name} . "\n";
            }
    
            # Loop through each value from the input and compare it to the corresponding value
            # obtained from the database. If any of the input values are different, then perform
            # an update, otherwise this row will be skipped and no action taken for this row.
            for my $k (keys(%inputValues)) {
                if (compareValues($inputValues{$k}, $DBvalues{$k}, $DBtypes{$k})) {
                    print STDERR "new value found: " . "input: " . $inputValues{$k} . " db: " . $DBvalues{$k} . "\n", if ($verbose);
                    $doUpdate = 1;
                    last;
                }
            }

            # An input value was different than the corresponding value in the db, so perform the update.
            if ($doUpdate) {
                $sthTxn = $self->genUpdateStmt($tableName, $keyColumnsRef, $colNamesRef, $colValuesRef);
                $self->rowsUpdatedCount($self->rowsUpdatedCount + 1);
            }
        } else {
            # If a row does not exists, then insert a new row
            $doInsert = 1;
            $sthTxn = $self->genInsertStmt($tableName, $keyColumnsRef, $colNamesRef, $colValuesRef);
            $self->rowsInsertedCount($self->rowsInsertedCount + 1);
        }
    }

    # If this row is not to be skippeed then the appropriate update, insert or delete statement has been constructed.
    # Now we will execute the statement and update the corresponding counters.
    if ($doUpdate or $doInsert or $doDelete) {
        # This will be the first row we are trying to update or insert so let the user know that
        # the transaction is open. The transaction is actually started when the txn statement
        # handle is created.
        print STDERR "Beginning database transaction.\n",  if ($self->stmtCount == 0 and $verbose);
        $self->stmtCount($self->stmtCount + 1);
        # Execute the sql that the statement handle contains. The transaction that this statement
        # belongs to will be committed when "closeMB" is called.
        print STDERR "Sending SQL: " . $sthTxn->{Statement}, if $verbose;
        my $icnt = 0;
    
        # Print out the values that were sent to the prepare statment and will be substituted
        # by the database. There seems to be no way to query the database to get the actual
        # SQL statement that was executed.
        if ($verbose) {
            print STDERR " (";
            for my $k (sort(keys($sthTxn->{ParamValues}))) {
               my $sep = $icnt == 0 ? "" : ",";
               print STDERR $sep . '$' . $k . "=" . $sthTxn->{ParamValues}{$k};
               $icnt++;
            }
            print STDERR ")\n";
        }
        
        eval {
            $rc = $sthTxn->execute();
        };

        if ($@) {
            # $sthTxn->err and $DBI::err will be true if error was from DBI
            if ($self->stmtCount > 0) {
                print STDERR "Rolling back transaction.\n";
                $self->dbhtxn->rollback;
                die "Exiting...\n";
            }
            #warn $@->getErrorMessage();  
        }

        # If we are doing a delete, and the number of rows is greater than the max allowed, stop the transaction.
        # This is just a sanity check, as we should never be able to delete one row at a time.
        if ($doDelete and $rc > $MAX_ROWS_ALLOWED_PER_DELETE) {
            if ($self->stmtCount > 0) {
                print STDERR "Rolling back transaction.\n";
                $self->dbhtxn->rollback;
            }
            die "Internal Error: Maximum allowed rows to be deleted has been exceeded, max = " . $MAX_ROWS_ALLOWED_PER_DELETE . ", rows deleted = " . $rc . "\n";
        }
    } else {
        $self->rowsSkippedCount($self->rowsSkippedCount + 1);
        print STDERR "Skipping duplicate row in table " . '"' . $tableName . '"' . " with name=value: " . $inputRow . "\n", if $verbose;
    }

    #print "sthTxn err: " . $sthTxn->err;
    #print "DBI err: " . $DBI::err;
}

sub closeDB {

    my $self = shift;
    my $verbose = shift;

    if ($self->rowsUpdatedCount > 0 or $self->rowsInsertedCount > 0 or $self->rowsDeletedCount > 0) {
        print STDERR "Commiting database transaction.\n", if $verbose;
        # Close the database transaction
        eval {
            $self->dbhtxn->commit;   # commit the changes if we get this far
        };
  
        if ($@) {
            die "Transaction aborted because $@";
            #print $@->getErrorMessage();  
            # now rollback to undo the incomplete changes
            # but do it in an eval{} as it may also fail
            #eval { $self->dbhtxn->rollback };
            # add other application on-error-clean-up code here
        }
    }

    if ($verbose) {
        print STDERR $self->rowsDeletedCount . " rows deleted\n";
        print STDERR $self->rowsUpdatedCount . " rows updated\n";
        print STDERR $self->rowsInsertedCount . " rows inserted\n";
        print STDERR $self->rowsSkippedCount . " input rows skipped\n";
    }
}

sub genSelectStmt {

    # Construct an SQL 'select' statement from the data values passed in.
    # A DBI statement handle is returned so that the caller can execute
    # the select.
    my $self = shift;

    my $tableName = shift;
    my $keyColsRef = shift;
    my $namesRef = shift;
    my $valuesRef = shift;

    my %keyCols;
    my $whereClauseStr = "";
    my $valueStr  = "";
    my $colNameStr = "";
    my $selectStr = "";
    my $sqlStr = "";
    my $sth;

    # Construct the select statement by assembling each clause
    $selectStr = "select * from " . '"' . $tableName . '"';
    # Put key columns in a hash for easy access.
    for my $k (@$keyColsRef) {
        $k =~ s/^"//;
        $k =~ s/"$//;
        #print "key col: " . $k . "\n";
        $keyCols{$k} = 1;
    }

    my $whereClause = "";
    my @values;

    # Search through the key columns and include each key value in the select.
    my $it = each_array( @$namesRef, @$valuesRef );
    while ( my ($name, $value) = $it->() ) {
        if (exists ($keyCols{$name})) {
            if ($whereClause eq "" ) {
                $whereClause .= 'where ' . '"' . $name . '"' . '= ?';
            } else {
                $whereClause .= ' and ' . '"' . $name . '"' . '= ?';
            }

            #print "pushing: " . $value . "\n";
            push(@values, $value);
        }
    }

    $sqlStr .= $selectStr . " " . $whereClause  . ";";
    $sth = $self->dbh->prepare($sqlStr);

    # Bind the key column values to the statement handle place holders that were placed
    # above.
    my $i;
    for ($i=1; $i<= @values; $i++) {
        $sth->bind_param($i, $values[$i-1]);  # placeholders are numbered from 1
    }

    # Return the statmenent handle
    return $sth;
}

sub genInsertStmt {

    # Generate an 'insert' statement for the specified data.
    my $self = shift;
    my $tableName = shift;
    my $keyColsRef = shift;
    my $namesRef = shift;
    my $valuesRef = shift;

    my $ivar;
    my @values;
    my %keyCols;
    my $valueStr  = "";
    my $colNameStr = "";
    my $sqlStr;
    my $sth;

    $sqlStr .= "insert into " . '"' . $tableName . '"';
    # Put key columns in a hash for easy access.
    for my $k (@$keyColsRef) {
        $k =~ s/^"//;
        $k =~ s/"$//;
        $keyCols{$k} = 1;
    }

    my $it = each_array( @$namesRef, @$valuesRef );
    $ivar = 0; 
    while ( my ($name, $value) = $it->() ) {
        $ivar++;
        if ($colNameStr eq "") {
           $colNameStr = '(' . '"' . $name . '"';
        } else {
           $colNameStr .= ', ' . '"' . $name . '"';
        }

        if ($valueStr  eq "") {
           $valueStr = ' values ( $' . $ivar;
        } else {
           $valueStr .= ', $' . $ivar;
        }
        push (@values, $value);
    }

    $colNameStr .= ')';
    $valueStr  .= ')';

    $sqlStr .= $colNameStr . " " . $valueStr  . ";";

    $sth = $self->dbhtxn->prepare($sqlStr);

    my $i;
    for ($i=1; $i<= @values; $i++) {
        $sth->bind_param('$' . $i, $values[$i-1]);  # placeholders are numbered from 1
    }

    # Return the statmenent handle
    return $sth;

}

sub genUpdateStmt {
 
    # Generate an 'update' statement for the specified data.
    my $self = shift;
 
    my $ivar;
    my $tableName = shift;
    my $keyColsRef = shift;
    my $namesRef = shift;
    my @values;
    my $valuesRef = shift;
 
    my %keyCols;
    my $whereClause = "";
    my $setStr = "";
    my $sqlStr;
    my $sth;
 
    $sqlStr .= "update " . '"' . $tableName . '"';
    # Put key columns in a hash for easy access.
    for my $k (@$keyColsRef) {
        $k =~ s/^"//;
        $k =~ s/"$//;
        $keyCols{$k} = 1;
    }
 
    # Add each field to the SQL update statement
    my $it = each_array( @$namesRef, @$valuesRef );
    $ivar = 0; 
    while ( my ($name, $value) = $it->() ) {
        $ivar++;
        # Add each primary key column to the 'where' clause
        if (exists ($keyCols{$name})) {
            if ($whereClause eq "" ) {
                $whereClause .= 'where ' . '"' . $name .'"' . '= $' . $ivar;
            } else {
                $whereClause .= ' and ' . '"' . $name . '"' . '= $' . $ivar;
            }
        # Not a key column, so just update it's value
        } else {
            if ($setStr eq "") {
               $setStr = ' set ' . '"' . $name . '"' . '= $' . $ivar;
            } else {
               $setStr .= ', ' . '"' . $name . '"' . '= $' . $ivar;
            }
        }
        push(@values, $value);
    }
 
    $sqlStr .= $setStr . " " . $whereClause . ";";
 
    $sth = $self->dbhtxn->prepare($sqlStr);
    # Bind the key column values to the statement handle place holders that were placed
    # above.
    my $i;
    for ($i=1; $i<= @values; $i++) {
        $sth->bind_param('$' . $i, $values[$i-1]);  # placeholders are numbered from 1
    }
 
    return $sth;
} 

sub genDeleteStmt {

    # Generate a 'delete' statement for the specified data.
    my $self = shift;

    my $ivar;
    my $tableName = shift;
    my $keyColsRef = shift;
    my $namesRef = shift;
    my @values;
    my $valuesRef = shift;

    my %keyCols;
    my $whereClause = "";
    my $setStr = "";
    my $sqlStr;
    my $sth;

    $sqlStr .= "delete from " . '"' . $tableName . '"';
    # Put key columns in a hash for easy access.
    for my $k (@$keyColsRef) {
        $k =~ s/^"//;
        $k =~ s/"$//;
        $keyCols{$k} = 1;
    }

    # Add each key field to the SQL delete statement
    my $it = each_array( @$namesRef, @$valuesRef );
    $ivar = 0; 
    while ( my ($name, $value) = $it->() ) {
        $ivar++;
        # Add each primary key column to the 'where' clause
        if (exists ($keyCols{$name})) {
            if ($whereClause eq "" ) {
                $whereClause .= 'where ' . '"' . $name .'"' . '= $' . $ivar;
            } else {
                $whereClause .= ' and ' . '"' . $name . '"' . '= $' . $ivar;
            }
            push(@values, $value);
        } 
    }

    $sqlStr .= $setStr . " " . $whereClause . ";";

    $sth = $self->dbhtxn->prepare($sqlStr);
    # Bind the key column values to the statement handle place holders that were placed
    # above.
    my $i;
    for ($i=1; $i<= @values; $i++) {
        $sth->bind_param('$' . $i, $values[$i-1]);  # placeholders are numbered from 1
    }

    return $sth;
}

sub getKeyColumns {
    my $self = shift;

    my $tableName = shift;
    my $href;
    my $sth;
    my @keyColumns = ();

    $sth = $self->dbhtxn->primary_key_info( undef, undef, $tableName);
    $href = $sth->fetchrow_hashref;

    while (defined $href) {
        push(@keyColumns, $href->{'COLUMN_NAME'});
        #$tableName = $href->{'pg_table'};
        #print Dumper($href);
        $href = $sth->fetchrow_hashref;
    }

    return \@keyColumns;
}

sub submitSQL {

    my $self = shift;
    my $sqlStr = shift;
    my $verbose = shift;

    my $sth;
    my $rc;
    my @values;

    $sth = $self->dbh->prepare($sqlStr);
    $rc = $sth->execute;

    @values = $sth->fetchrow_array;

    return $values[0];

}

sub compareValues {

    # We are comparing input values from an external file to values from the Metabase2
    # database (in order to determine if the Metabase values need to be updated, as is 
    # the case if the values are different). Currently the input values come from a
    # external XML file that was created by the PostgreSQL XML functions. The database
    # values we are comparing them to come from the Perl DBD:Pg driver. A problem arises
    # where these two methods create values that are represented slightly differently.
    # For example, XML export represents boolean values as the strings "true" or "false",
    # where DBD::Pg represents booleans as the numeric value 1 or 0.

    # This method normalizes the comparison so that we can compare values with two different data 
    # representations for equivalence.
    #
    my $inputValue = shift;
    my $DBvalue = shift;
    my $DBtype = shift;

    # Column types supported by DBD::Pg include the following :
    #     boolean, character varying(50), integer, timestamp without time zone, date, ...

    my $dtInput;
    my $dtDB;
    #print "db value: " . $DBvalue . "\n";
    #print "db col type: " . $DBtype . "\n";
    if ($DBtype eq "boolean") {
        return 0, if lc($inputValue) eq "true" and $DBvalue;
        return 0, if lc($inputValue) eq "false" and not $DBvalue;
        return 1;
    } elsif ($DBtype eq "timestamp without time zone" or $DBtype eq "date" ) {
        # If the input value or the db value appear to be datetime values in 8601
        # format, then compare them as datetime values and not as strings. These
        # values may differ slightly in format as strings, but may be equivalent datetime values,
        # i.e.  "2013-06-20 00:00:00" vs "2013-06-20T00:00:00". Sooo, we don't want to update
        # the row if the datetimes are actually equivalent.

        $dtInput = parseDateTime($inputValue);
        $dtDB    = parseDateTime($DBvalue);
        return 1, if ($dtInput ne $dtDB);
    } else {
        # Don't know how to handle whatever input type these are, so just do straing string comparison
        return 1, if ($inputValue ne $DBvalue);
    }
    return 0;
}

sub parseDateTime {

    # Parse a string representation of a date or date/time and convert to
    # a Perl datetime.

    my $dtStr = shift;

    my $dt;
    my $year = $1;
    my $month = $2;
    my $day = $3;
    my $hour = $4;
    my $minutes = $5;
    my $seconds = $6;

    # the row if the datetimes are actually equivalent.
    if ($dtStr =~ /(\d{4})[-\/](\d{2})[-\/](\d{2})[T\s](\d{2}):(\d{2}):(\d{2})/) {
        # Parse the input datetime string and create a Perl DateTime object
        $year = $1;
        $month = $2;
        $day = $3;
        $hour = $4;
        $minutes = $5;
        $seconds = $6;

        $dt = DateTime->new(
            year       => $year,
            month      => $month,
            day        => $day,
            hour       => $hour,
            minute     => $minutes,
            second     => $seconds,,,);
    } elsif ($dtStr =~ /(\d{4})[-\/](\d{2})[-\/](\d{2})/) {
        # Parse the input date string and create a Perl DateTime object
        $year = $1;
        $month = $2;
        $day = $3;

        $dt = DateTime->new(
            year       => $year,
            month      => $month,
            day        => $day,,,,,,);
    } else {
        die "Internal error: Unknown date/time format for value: " . $dtStr . "\n";
    }

    return $dt;
}

# Make this Moose class immutable
__PACKAGE__->meta->make_immutable;

1;
