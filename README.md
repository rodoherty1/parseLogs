Parse Logs
==========

Overview
--------
This project is starting out as a log file parser to help me understand how jobs in my Hadoop cluster are running.

It currently scans a selection of log files (idenified by a glob filter) and writes out a bit of a long-winded summary of each job that was run.  Currently, the output is so long-winded it isn't particularly useful but I'll change that when I work out exactly what I need of the output.


Todo
----
* Record database sizes and other stats about the file system
