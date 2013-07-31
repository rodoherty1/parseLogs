
var grep = require('grep1'),
	_= require('underscore'),
	$ = require('jquery'),
	reportProcessorAsync = require('async');


var JobExecutions = function () {
	'use strict';
	
	var jobExecutions = [];

	this.addJobExecution = function(startTime, endTime, recordsRead, recordsWritten) {
		
		var jobExecution = {
			startTime : startTime,
			endTime : endTime,
			recordsRead : recordsRead,
			recordsWritten : recordsWritten
		};		
		
		jobExecutions.push(jobExecution);
	};
	
	this.countJobExecutions = function () {
		return jobExecutions.length;
	};
	
};


var ReportProcessor = function () {
	'use strict';
	
	var results = {};
	var element = {};
	var startTimeAndEndTime;
	
	var getReportName = function(element) {
		console.log ('getRedportName()');

		var reportName = '';
		var matches = element.match(/Successfully stored.*/g);
		
		for (var i=0; i<matches.length; i++) {
			// /\/(?!latest\-known)[^\/]+$/ 
			var match = matches[i].substring(matches[i].lastIndexOf("/") + 1, matches[i].length-1);
			if (match && !match.match(/latest-known/)) {
				if (reportName.length > 0) {
					reportName += '___';
				}
				reportName += match;
			}
		}
		
		return reportName;
	};
	
	var getStartTimeAndEndTime = function(callback) {
		
		var startTime = 1, endTime = 2;
		
		var optionsGrepStartTime = ['-B', '2', '^Success\!$'];
		grep(optionsGrepStartTime.concat([element]), function(err, result, stderr) {
			if (err || stderr) {
				console.log ('Err - getStartTimeAndEndTime()');
				callback(err, 'getStartTime');
				return 0;
			} else {
				console.log ('result - getStartTimeAndEndTime()');
				startTimeAndEndTime = {
					"startTime" : startTime,
					"endTime" : endTime
				};

				callback(null, 'getStartTime');
				return 1;
			}
		});
		
	};	

	var getEndTime = function(element) {
		return 2;
	};	

	var getRecordsRead = function(element) {
		return 3;
	};	

	var getRecordsWritten = function(element) {
		return 4;
	};	
		
	var fireStartAndEndTime = function() {
		reportProcessorAsync.series([
			getStartTimeAndEndTime
		],
		function(err, results){
			if (err) {
				console.log('Errors encountered - ' + err);
			}
			
			console.log(results);
		});
	};
		
	var addToResultsCollection = function(e, index, list) {
		element = e;
		fireStartAndEndTime();
		var reportName = getReportName(element);
		var recordsRead = getRecordsRead(element);
		var recordsWritten = getRecordsWritten(element);

		var startTime = startTimeAndEndTime.startTime;
		var endTime = startTimeAndEndTime.endTime;

		
		var reportJobExecutions = results[reportName];
		
		if (!reportJobExecutions) {
			reportJobExecutions = new JobExecutions();
			results[reportName] = reportJobExecutions;
		}
		
		reportJobExecutions.addJobExecution(startTime, endTime, recordsRead, recordsWritten);
	};
	
	var printCount = function(value, key, list) {
		console.log (key + ' : ' + value.countJobExecutions());
	};
	

	var countSuccessfulReports = function(result) {
		var lines = result.split("--\n");
		_.each(lines, addToResultsCollection);
		//_.each(results, printCount);
	};
		
		
	var grepLogs = function(config) {
		var optionsAllSuccessfulReports = ['-B', '17', '-A', '5', '^Total records written'];

		grep(optionsAllSuccessfulReports.concat([config.reportProcessor.logFile]), function(err, result, stderr) {
			if (err || stderr) {				
				return err;
			} else {
				countSuccessfulReports(result);
				return null;
			}
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



