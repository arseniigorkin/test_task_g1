#!/usr/bin/env perl -w

use strict;
use DBI;

########################################################
## This is a small but brave parser ####################
## Author: Arsenii Gorkin ##############################
########################################################
## This parser reads though the given log file line by
## line to prevent memory leaks
## If some records does not have message_id info those
## records will be dumped to the broken_records_dump.txt
## file for further actions
########################################################
## This parser accepts filename as only argument #######
## Usage example: perl parser.pl '/path/to/file' #######
########################################################

my $file = $ARGV[0] || "out"; # filename to process
$file =~ s/\n\t//g;

#DB's vars
my ($dsn, $dbh, $sth, $rv);

#a DB config
my ($db_name, $db_host, $db_port, $db_username, $db_password);
	$db_name = 'test_db';
	$db_host = '127.0.0.1';
	$db_port = '5432';
	$db_username = 'username';
	$db_password = '1234567';

#a DB connect sub
sub DB_CONNECT {
	
	$dsn = "DBI:Pg:dbname=".$db_name.";host=".$db_host.";port=".$db_port.";sslmode=require";
	$dbh = DBI->connect($dsn, $db_username, $db_password, { RaiseError => 0 }) or die "Could not connect to the DB: $DBI::errstr";
	$dbh->{pg_enable_utf8} = 1;
	
}

#a DB disconnect sub
sub DB_DISCONNECT {
	
	$rv = $dbh->disconnect or die "Could not close connection to the DB: $DBI::errstr";
}

#a main processing sub for each line of the log file
sub process_line {
	
	my $full_line = shift;
	my %record; #main record's data container
	
	#NB: however there are many RFC compliant ways to validate an email address, here I used a primitive (valid exclusively for current task) test for addresses of the given in the log fime formats. In real life, there ought to be used a correct RFC compliant way for email addresses validation.
	$full_line =~ /(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) ([a-zA-Z0-9\-]+) ([=<>\-*]{2})? ?([^ @]+@[^ ]+)? ?(.*)?/;
	$record{full_line} = $full_line; #for easy dumpimg of broken records purpose
	$record{created} = $1;
	$record{int_id} = $2;
	$record{flag} = $3 ? $3 : "Null"; #Null to prevent undefs in the DB query
	$record{address} = $4 ? $4 : "Null"; #Null to prevent undefs in the DB query
	my $tempStr = $5;
	$record{str} = $record{address} eq "Null" ? " ".$tempStr : $record{address}." ".$tempStr;
	undef $tempStr; #clearing memory addr to prevent cloning data in the DB in the next iterations.
	($record{message_id}) = ($record{str} =~ /id=([\w\-\.\@]*)/) ? ($record{str} =~ /id=([\w\-\.\@]*)/) : ("Null"); #Null to prevent undefs in the DB query
		
	#starting insertion of the log file line
	#for incoming message
	if ($record{flag} and $record{flag} eq "<=") {
		
		insert_message(\%record);
		
	}
	
	#for a log (rest types of records)
	else {
		
		insert_log(\%record);
		
	}
	
	undef %record;
			
}

#a sub for inserting results for an incoming message into the RDBS
sub insert_message {
	
	my $record = shift;
	
	if (ref($record) eq "HASH") {
		
		#checking if the record has a valid message_id (because of the DB's schema mandatory)
		if ($$record{message_id} eq "Null") {
			
			my $brd_file = "broken_records_dump.txt";
			
			open(BRD, ">>", $brd_file) or die "Could not open the $brd_file for record.";
			
			print BRD $$record{full_line};
			
			close BRD;
			
			print "Could not process the record number $. because of lack of \'message_id\' field (RDBS schema mandatory). The record is dumped into the <$brd_file>.\n";
			
		}
		
		else {
	
			my $statement =<<SQL;
			INSERT INTO message (
				created,
				int_id,
				str,
				id
				)
			VALUES (?,?,?,?);
SQL

			$sth = $dbh->prepare($statement);
			$sth->execute($$record{created}, $$record{int_id}, $$record{str}, $$record{message_id}) or print "Error in the message insertion of the record number $.: $dbh->errstr";
			
		} # else (good cond) for message_id check
		
	}
	
	else {
		
		print "Could not process the line number $. as a message. No data have been given. Expected HASH REF.";
		
	}
	
}

#a sub for inserting results for a log into the RDBS
sub insert_log {
	
	my $record = shift;
	if (ref($record) eq "HASH") {
		
		my $statement =<<SQL;
		INSERT INTO log (
				created,
				int_id,
				str,
				address
				)
		VALUES (?,?,?,?);
SQL
	
		$sth = $dbh->prepare($statement);
		$sth->execute($$record{created}, $$record{int_id}, $$record{str}, $$record{address}) or print "Error in the log insertion of the record number $.: $dbh->errstr";
		
	}
	
	else {
		
		print "Could not process the line number $. as a log. No data have been given. Expected HASH REF.";
		
	}
}


print "Opening your logs file...\n";
open (LOG, $file) or die "Sorry, I could not cope with opening your logs file. Try to specify it as: perl parser.pl '/path/to/file'\n";
	
	#connecting to the DB
	DB_CONNECT;
	
	print "File with logs successfylly opened.\n";
	print "Starting parsing...\n\n";
	
	#leafing through the file line by line
	while (<LOG>) {
		
		process_line $_;
		
	}
	
	#Finilizing the DB process
	$sth->finish;
	
	#disconnecting from the DB
	DB_DISCONNECT;
	
close LOG;

print "Done! We have survived!\n";
