Parse Logs
==========

Overview
--------
This project is log file parser to help me understand how jobs in my Hadoop cluster are running.

It currently scans a selection of log files (identified by a glob filter) and outputs the following: 
 - a human-readable summary of each job that was run
 - a CSV file for each type of Hadoop job.

Each CSV file is ready for importing into some kind of charting tool (e.g. DyGraph or R) so that the performance of each job can be visualised.


The config.json file describes the parameters that are used during the parsing of the log files.

For now, the config.json file is only used to specify a date range.  More options can be added as needed.


Todo
----
* Record database sizes and other stats about the file system.  These metrics are currently available using a separate suit of Bash scripts so consider invoking these scripts from this application or porting the scripts to Javascript.


