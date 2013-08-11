Parse Logs
==========

Overview
--------
This project is starting out as a log file parser to help me understand how jobs in my Hadoop cluster are running.

It currently scans a selection of log files (identified by a glob filter) and writes out a summary of each job that was run.

The config.json file describes the parameters that are used during the parsing of the log files.

Todo
----
* Record database sizes and other stats about the file system
