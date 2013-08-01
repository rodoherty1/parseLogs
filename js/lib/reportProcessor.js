
var grep = require('grep1'),
	_= require('underscore'),
	$ = require('jquery'),
	reportProcessorAsync = require('async'),
	through = require('through'),
	split = require('split'),
	fs = require('fs'),
	moment = require('moment');


var JobExecutions = function () {
	'use strict';
	
	var jobExecutions = [];

	this.addJobExecution = function(startTime, endTime, recordsRead, recordsWritten) {
		var jobExecution = {
			startTime : startTime,
			endTime : endTime,
			runningTime : endTime - startTime,
			recordsRead : recordsRead,
			recordsWritten : recordsWritten
		};		
		
		jobExecutions.push(jobExecution);
	};
	
	this.countJobExecutions = function () {
		return jobExecutions.length;
	};

	this.averageRunningTime = function () {
		var totalRunningTime = 0;
		
		for (var i=0; i<jobExecutions.length; i++) {
			totalRunningTime += jobExecutions[i].runningTime;
		}
	
		return totalRunningTime / jobExecutions.length;
	};
};


var ReportProcessor = function () {
	'use strict';
	
	var results = {};
	var element = {};
	
	var dbTableRecordsWrittenTo=[], startTimeInMS, endTimeInMS, runningTimeInMS, recordsRead=[], recordsWritten=[];
	
	/*
	 * Parser state
	 */
	var skipNextXLines = 0;
	var nextParserState = readUntilStartOfJobExecution;
	var nextParserStates = [];


	var skipNextLine = function () {
		skipNextLines(1);
	};
	
	var skipNextLines = function (linesToSkip) {
		skipNextXLines = linesToSkip;
		andThen(skipLine);
	};
	
	var andThen = function(nextState) {
		nextParserStates.push(nextState);
	};
	
	/*
	 * Only used during development to force the parser to stop after a particular step
	 */
	var doNothing = function(s) {
		return;
	};
	
	var readUntilStartOfJobExecution = function (s) {
		if (s.match(/Script Statistics\:/)) {
			console.log ('Found start of JobExecution block: ' + s);
			nextParserStates.length = 0;
			skipNextLines(2);
			andThen(parseStartAndEndDate);
		}
	};
	
	var createReportName = function () {
		var reportName;
		
		reportName = dbTableRecordsWrittenTo[0];
		
		for (var i=1; i<dbTableRecordsWrittenTo.length; i++) {
			reportName = reportName + '___' + dbTableRecordsWrittenTo[i];
		}
		
		return reportName;
	};
	
	
	var storeJobExecution = function() {
		var reportName = createReportName();
		var reportJobExecutions = results[reportName];
		
		if (!reportJobExecutions) {
			reportJobExecutions = new JobExecutions();
			results[reportName] = reportJobExecutions;
		}
		
		reportJobExecutions.addJobExecution(startTimeInMS, endTimeInMS, recordsRead, recordsWritten);
		
		dbTableRecordsWrittenTo.length = 0, startTimeInMS = {}, endTimeInMS = {}, recordsRead.length = 0, recordsWritten.length = 0;
	};
	
	
	var readUntilSuccessfullyStoredData = function (s) {
		// 'Successfully stored 36223 records (1485311 bytes) in: "hdfs://10.198.10.10:8020//known_spammers//topSMSSpammers/latest-known-spammers-1374844338731"'
		var successfullyStoredRegex = /Successfully stored/;
		var matches = s.match(successfullyStoredRegex);
		
		if (matches) {
			readSuccessfullyStoredData(s);
			nextParserStates.length = 0;
			andThen(readSuccessfullyStoredData);
		}
	};
	

	var readSuccessfullyStoredData = function (s) {
		// 'Successfully stored 36223 records (1485311 bytes) in: "hdfs://10.198.10.10:8020//known_spammers//topSMSSpammers/latest-known-spammers-1374844338731"'

		var successfullyStoredRegex = /Successfully stored/;
		var dbTableRecordsWrittenToRegex = /\/(?!latest\-known)[^\/]+$/;
		var recordsStoredRegex = /([0-9]+)/;
		
		var dbTableMatches;
		var matches = s.match(successfullyStoredRegex);
		
		if (matches) {
			dbTableMatches = s.match(dbTableRecordsWrittenToRegex);
			if (dbTableMatches) {
				dbTableRecordsWrittenTo.push(dbTableMatches[0].substring(1, dbTableMatches[0].length-1));
				
				matches = s.match(recordsStoredRegex);
				if (matches) {
					recordsWritten.push(matches[0]);
					console.log ('Successfully stored ' + matches[0] + ' records');
				}
			}
		} else {
			nextParserStates.length = 0;
			storeJobExecution();
			andThen(readUntilStartOfJobExecution);
		}
	};
	
	
	var readUntilSuccessfullyReadData = function (s) {
		var regex = /^Successfully read ([0-9]+) records.*\/data"$/;
		
		if (s.match(regex)) {
			regex = /([0-9]+)/;
			var matches = s.match(regex);
			if (matches) {
				recordsRead.push(matches[0]);
				console.log ('Successfully read ' + matches[0] + ' records');
				nextParserStates.length = 0;
				andThen(readUntilSuccessfullyStoredData);
			}
		}
	};
	

	var confirmSuccessfulJobExecution = function(s) {
		if (s === 'Success!') {
			console.log ('Confirmed Successful JobExecution');
			nextParserStates.length = 0;
			andThen(readUntilSuccessfullyReadData);			
		}
	};
	
	
	var parseStartAndEndDate = function (s) {
		console.log ('Parsing out Start and End dates from "' + s + '"');
		var dateTimeRegex = /(([1-2][0-9][0-9][0-9])-([0][1-9]|[1][0-2])-([0][1-9]|[1-2][0-9]|[3][0-1])\s([0-1][0-9]|[2][0-3]):([0-5][0-9]):([0-5][0-9]))+/g;
		var match = dateTimeRegex.exec(s);
		startTimeInMS = moment(match[0], 'YYYY-MM-DD HH:mm:ss').unix();

		match = dateTimeRegex.exec(s);
		endTimeInMS = moment(match[0], 'YYYY-MM-DD HH:mm:ss').unix();

		nextParserStates.length = 0;
		skipNextLines(1);
		andThen(confirmSuccessfulJobExecution);
	};
	
	var skipLine = function (s) {
		console.log ('Skipping: ' + s);
		skipNextXLines-=1;
		
		if (skipNextXLines === 0) {
			nextParserStates.shift();
		}
	};
	
	
	
	var parseLine = function(buf) {
		nextParserStates[0](buf.toString());
	};
	


	
//	var getReportName = function(element) {
//		console.log ('getRedportName()');
//
//		var reportName = '';
//		var matches = element.match(/Successfully stored.*/g);
//		
//		for (var i=0; i<matches.length; i++) {
//			// /\/(?!latest\-known)[^\/]+$/ 
//			var match = matches[i].substring(matches[i].lastIndexOf("/") + 1, matches[i].length-1);
//			if (match && !match.match(/latest-known/)) {
//				if (reportName.length > 0) {
//					reportName += '___';
//				}
//				reportName += match;
//			}
//		}
//		
//		return reportName;
//	};
	
//	var getStartTimeAndEndTime = function(callback) {
//		
//		var startTime = 1, endTime = 2;
//		
//		var optionsGrepStartTime = ['-B', '2', '^Success\!$'];
//		grep(optionsGrepStartTime.concat([element]), function(err, result, stderr) {
//			if (err || stderr) {
//				console.log ('Err - getStartTimeAndEndTime()');
//				callback(err, 'getStartTime');
//				return 0;
//			} else {
//				console.log ('result - getStartTimeAndEndTime()');
//				startTimeAndEndTime = {
//					"startTime" : startTime,
//					"endTime" : endTime
//				};
//
//				callback(null, 'getStartTime');
//				return 1;
//			}
//		});
//		
//	};	
//
//	var getEndTime = function(element) {
//		return 2;
//	};	
//
//	var getRecordsRead = function(element) {
//		return 3;
//	};	
//
//	var getRecordsWritten = function(element) {
//		return 4;
//	};	
		
//	var fireStartAndEndTime = function() {
//		reportProcessorAsync.series([
//			getStartTimeAndEndTime
//		],
//		function(err, results){
//			if (err) {
//				console.log('Errors encountered - ' + err);
//			}
//			
//			console.log(results);
//		});
//	};
		
//	var addToResultsCollection = function(e, index, list) {
//		element = e;
//		fireStartAndEndTime();
//		var reportName = getReportName(element);
//		var recordsRead = getRecordsRead(element);
//		var recordsWritten = getRecordsWritten(element);
//
//		var startTime = startTimeAndEndTime.startTime;
//		var endTime = startTimeAndEndTime.endTime;
//
//		
//		var reportJobExecutions = results[reportName];
//		
//		if (!reportJobExecutions) {
//			reportJobExecutions = new JobExecutions();
//			results[reportName] = reportJobExecutions;
//		}
//		
//		reportJobExecutions.addJobExecution(startTime, endTime, recordsRead, recordsWritten);
//	};
	

//	var countSuccessfulReports = function(result) {
//		var lines = result.split("--\n");
//		_.each(lines, addToResultsCollection);
//		//_.each(results, printCount);
//	};
		
		
	var printJobExecution = function(value, key, list) {
		var summary = {
			reportName : key,
			numberOfExecutions : value.countJobExecutions(),
			averageRunningTime : value.averageRunningTime()
		};
			
		console.log (summary);
	};
	
	var printSummary = function() {
		_.each(results, printJobExecution);
	};
		
	var grepLogs = function(config) {
		nextParserStates.push(readUntilStartOfJobExecution);
		var stream = fs.createReadStream(config.reportProcessor.logFile);
		var streamSplitByCarriageReturns = stream.pipe(split());
		var parser = streamSplitByCarriageReturns.pipe(through(parseLine));
		
		parser.on('end', function() {
			console.log('Finished reading ' + config.reportProcessor.logFile + '!');
			printSummary();
		});

	};
	
	exports.process = function(config, callback) {
		var err = grepLogs(config, callback);
		
		if (err) {
			callback(err, 'process');
		} else {
			callback(null, 'process');
		}
	};
};

exports.reportProcessor = new ReportProcessor();



