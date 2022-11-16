#!/usr/bin/perl -w

################################################################
## This is a primitive mail listing CGI app ####################
## Author: Arsenii Gorkin ######################################
################################################################
## To use it place this file to the "cgi-bin" dir ##############
## inside your WEB-server's cgi-bin dir ########################
## and chmod this file to 755 ##################################
################################################################

use strict;
use CGI::Carp qw(fatalsToBrowser);
use CGI qw/:standard/;
use DBI;
use v5.10;

print "Content-type:text/html\n\n";

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

##### Main operations #####

#processing CGI query
my $email = param("email");
$email =~ s/[^\w\-\+\~\@\.]//ig; # cleaning email param from RFC incomincompliant characters

#this is a very simple "hand made" regex just for the current task. In real life, there ought to be used an RFC compliant regex.
if ($email and $email =~/^[a-z0-9]+([\w\-\+\~]*[a-z0-9]+)*(\.[a-z0-9]+[\w\-\+\~]*[a-z0-9]+)*(\.[a-z0-9]+)*@[a-z0-9]+([a-z0-9\-]*[a-z0-9]+)*(\.[a-z0-9]+[a-z0-9\-]*[a-z0-9]+)*(\.[a-z0-9]+)*((\.[a-z0-9]+[a-z0-9\-]*[a-z0-9]+)|(\.[a-z0-9]+))$/i) {
	
	DB_CONNECT;
	
	process_email();
	
	DB_DISCONNECT;
	
}

else {
	
	my $body =<<HTML;
		<h2>Ошибочка.</h2>
		<br>
		Укажите, пожалуйста, действительный адрес электронной почты.
		<br><br>
		<button onClick="javascript:window.history.back();">Попробовать снова</button>
HTML
	print_html_template({title=>'Ошибка', body=>$body});
	
}

##### Main subs #####

sub print_html_template {
	
	my $html = shift; #expecting HASH REF with 'body' and 'title' keys.
	
	if (ref($html) eq "HASH") {
		
		print <<HTML;
		<!DOCTYPE html>
		<html lang="ru-ru">
			<head>
				<meta charset="utf-8">
				<title>Тестовое задание | $$html{title}</title>
			<head>
			<body>
				$$html{body}
			</body>
		</html>
HTML
		
	} # if $html is HASH REF (IF)
	
	else {
		
		#printing error because sub has received no valid HASH REF
		print <<HTML;
	<!DOCTYPE html>
	<html lang="ru-ru">
		<head>
			<meta charset="utf-8">
			<title>Тестовое задание | Ошибка</title>
		<head>
		<body>
		<h2>Ошибочка.</h2>
		<br>
		Неверная настройка функции print_html_template.
		</body>
	</html>
	
HTML
		
	}
	
} #sub print_html_template

sub process_email {
	
	my $statement =<<SQL;
			SELECT count(*)
				FROM (
					SELECT str
						FROM message
						UNION ALL
					SELECT str
						FROM log
				) AS tempTable
				WHERE str LIKE ?
SQL

	$sth = $dbh->prepare($statement);
	$sth->execute("\%$email\%") or print "Error in the DB fetch (could not count total of rows): $dbh->errstr";
	my ($records_counter) = $sth->fetchrow_array();
		
	# if any records have been found
	if ($records_counter) {
		
		my ($lines_word, $body, $limit_text);
		given(substr $records_counter, -2, 2) {
			when(/^[^1]?[2-4]$/) {$lines_word = "строки";}
			when(/^[^1]?1$/) {$lines_word = "строка";}
			default {$lines_word = "строк";}
			
		} # given
		
		$limit_text = "&#9888; Установлен максимальный лимит на вывод в 100 строк" if $records_counter > 100;
		
		my $body .=<<HTML;
				<h2>Результат поиска по запросу: <font color="maroon"><i>$email</i></font></h2><br>
				<h3>Всего найдено <u>$records_counter</u> $lines_word</h3>
				<h4>$limit_text</h4>
				<table width="100%" border="1">
					<tr style="background-color: cornflowerblue; color: white; font-weight: bold;">
						<td align="center" style="min-width: max-content;">Номер строки</td>
						<td align="center" style="min-width: max-content;">Дата создания</td>
						<td align="center" style="min-width: max-content;">Внутренний номер</td>
						<td align="center">Сообщение</td>
					</tr>
HTML
		
		$statement =<<SQL;
				SELECT created, str, int_id
					FROM (
						SELECT created, str, int_id
							FROM message
							UNION
						SELECT created, str, int_id
							FROM log
					) AS tempTable
					WHERE str LIKE ?
					ORDER BY int_id ASC, created ASC;
SQL

		$sth = $dbh->prepare($statement);
		$sth->execute("\%$email\%") or print "Error in the DB fetch (could not get rows): $dbh->errstr";
		
		my $counter = 1;
		
		while (my $fetched_record = $sth->fetchrow_hashref) {
			
			#breaking if there is 101-th record
			last if $counter == 101;
			
			$body .=<<HTML;
					<tr>
						<td align="center" style="background-color: aliceblue; color: cornflowerblue;"><font size="2em">$counter</font></td>
						<td align="center"><font size="2em" style="min-width: max-content;">$$fetched_record{created}</font></td>
						<td align="center"><font size="2em" style="min-width: max-content;">$$fetched_record{int_id}</font></td>
						<td><font size="2em">$$fetched_record{str}</font></td>
					</tr>
HTML
			$counter++;
		
		} #while
		
		
		
		$body .=<<HTML;
				</table>
HTML

		print_html_template({title=>'Результат поиска', body=>$body});
		
	} # if any records found (IF)
	
	else {
		
		my $body =<<HTML;
		<h2>Увы и ах, но по запросу <font color="maroon"><i>$email</i></font> ничего не удалось найти.</h2>
		<br><br>
		<button onClick="javascript:window.history.back();">Попробовать снова</button>
HTML
		print_html_template({title=>'Результат поиска', body=>$body});
		
	}
	
	$sth->finish;


} #sub process_email
