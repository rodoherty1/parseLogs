
var grep = require('grep1'),
	_= require('underscore'),
	$ = require('jquery'),
	reportProcessorAsync = require('async'),
	through = require('through'),
	split = require('split'),
	fs = require('fs'),
	moment = require('moment'),
	log4js = require('log4js'),
	glob = require('glob');
	



var JobExecutions = function () {
	'use strict';


	/*
	 * Report Processor Results
	 */
	var jobExecutions = [];

	
	this.addJobExecution = function(startTime, endTime, recordsRead, recordsWritten) {
		var jobExecution = {
			startTimeInEpoch : startTime,
			endTimeInEpoch : endTime,
			runningTime : endTime - startTime,
			recordsRead : recordsRead.slice(0),
			recordsWritten : recordsWritten.slice(0)
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
	
	this.averageRecordsRead = function () {
		var totalRecordsRead = 0;
		
		for (var i=0; i<jobExecutions.length; i++) {
			totalRecordsRead += jobExecutions[i].recordsRead;
		}
	
		return totalRecordsRead / jobExecutions.length;
	};
	
	this.getJobExecutions = function() {
		return jobExecutions;
	};
};


var ReportProcessor = function () {
	'use strict';

	/*
	 * Log4JS Configuration
	 */
	var log4jConfFile = 'log4js.json';
	var logger = log4js.getLogger('mainLogger');
	
	/*
	 * Input config.json variables
	 */
	var configResultsDir = 'results';
	var configStartTimeInMS= 0;
	var configEndTimeInMS = (new Date()).getTime();
	
	/*
	 * Report Processor Results
	 */
	var results = {};
	var element = {};
	
	var dbTableRecordsWrittenTo=[], startTimeInMS, endTimeInMS, runningTimeInMS, recordsRead=[], recordsWritten=[];
	
	var resultsWriteStream;
	var subtotalRunningTime = 0;
	
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
			logger.debug ('Found start of JobExecution block: ' + s);
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
					logger.debug ('Successfully stored ' + matches[0] + ' records');
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
				logger.debug ('Successfully read ' + matches[0] + ' records');
				nextParserStates.length = 0;
				andThen(readUntilSuccessfullyStoredData);
			}
		}
	};
	

	var confirmSuccessfulJobExecution = function(s) {
		if (s === 'Success!') {
			logger.debug ('Confirmed Successful JobExecution');
			nextParserStates.length = 0;
			andThen(readUntilSuccessfullyReadData);			
		}
	};
	
	
	var parseStartAndEndDate = function (s) {
		logger.debug ('Parsing out Start and End dates from "' + s + '"');
		var dateTimeRegex = /(([1-2][0-9][0-9][0-9])-([0][1-9]|[1][0-2])-([0][1-9]|[1-2][0-9]|[3][0-1])\s([0-1][0-9]|[2][0-3]):([0-5][0-9]):([0-5][0-9]))+/g;
		
		var match = dateTimeRegex.exec(s);
		startTimeInMS = moment(match[0], 'YYYY-MM-DD HH:mm:ss').unix();
		
		match = dateTimeRegex.exec(s);
		endTimeInMS = moment(match[0], 'YYYY-MM-DD HH:mm:ss').unix();
		
		if ((startTimeInMS > configStartTimeInMS) && (endTimeInMS < configEndTimeInMS)) {
			nextParserStates.length = 0;
			skipNextLines(1);
			andThen(confirmSuccessfulJobExecution);
		} else {
			nextParserStates.length = 0;
			andThen(readUntilStartOfJobExecution);
		}
	};
	
	var skipLine = function (s) {
		logger.debug ('Skipping: ' + s);
		skipNextXLines-=1;
		
		if (skipNextXLines === 0) {
			nextParserStates.shift();
		}
	};

	
	var parseLine = function(buf) {
		nextParserStates[0](buf.toString());
	};
	

	function formatSeconds(seconds) {
		var secs = seconds % 60;
		var minutes = Math.floor(seconds / 60);
		var hours = Math.floor(seconds / (60 * 60));
		
		var s = '';
		if (hours) {
			s += hours + 'hrs ';
		}
		
		if (hours || minutes) {
			s += minutes + 'mins ';
		}

		if (hours || minutes || secs) {
			s += secs + 'secs';
		}
		
		return s;
	}

	var getJobExecutionSummary = function(value, key, list) {
		var summary = {
			reportName : key,
			numberOfExecutions : value.countJobExecutions(),
			averageRunningTime : formatSeconds(value.averageRunningTime()),
			averageRecordsRead : value.averageRecordsRead()
		};
		return summary;
	};


	
	var printJobExecution = function(value, key, list) {
		logger.info (getJobExecutionSummary(value, key, list));
	};

	
	var writeJobExecution = function(element, index, list) {
		//resultsWriteStream.write(JSON.stringify(element, null, '\t') + '\n');
		var s = moment.unix(element.startTimeInEpoch).format('YYYY-MM-DD HH:mm:ss') + '\t' + moment.unix(element.endTimeInEpoch).format('YYYY-MM-DD HH:mm:ss') + '\t' + element.runningTime + ' secs\t' + element.recordsRead + '\t' + element.recordsWritten + '\n';

		subtotalRunningTime += element.runningTime;
		resultsWriteStream.write(s);
		if (index == list.length-1) {
			resultsWriteStream.write('Summary: {Avg Running Time: ' + (subtotalRunningTime/list.length) + ' seconds}\n');
			subtotalRunningTime = 0;
		}
	};
	
	
	var writeJobExecutions = function(value, key, list) {
		resultsWriteStream.write('\n' + key + '\n');
		resultsWriteStream.write('startTime\t\tendTime\t\t\trunningTimeInSecs\trecordsRead\trecordsWritten\n');
		resultsWriteStream.write('=========\t\t=======\t\t\t=================\t===========\t==============\n');
		
		_.each(value.getJobExecutions(), writeJobExecution);
	};


	var writeJobExecutionsSummary = function(value, key, list) {
		resultsWriteStream.write(JSON.stringify(getJobExecutionSummary(value, key, list)));
	};


	var printSummary = function() {
		_.each(results, printJobExecution);	
	};
	
	var createNextResultsFilename = function(fileNamePrefix) {
		moment().format('MMMM Do YYYY, h:mm:ss a');
		return configResultsDir + '/' + fileNamePrefix + '_' + moment().format('YYYY_MM_DD[T]HH:mm:ss') + '.log';
	};
	
	var storeResults = function(callback) {
		var items = [storeReportProcessorDetailedResults, storeReportsProcessorResultsSummary];
		var results = [];
		
		items.forEach(function(item) {
			storeResultsAsync(item, function(result){
				results.push(result);
				if (results.length == items.length) {
					final(callback);
				}
			});
		});
	};
		
	var storeResultsAsync = function(storeFunction, storedResultsCallback) {
		storeFunction(storedResultsCallback);
	};
	
	var final = function(callback) {
		console.log('All results stored', results);
		callback(null, 'process');
	};

	

	var storeReportProcessorDetailedResults = function(callback) {
		var filename = createNextResultsFilename('reportProcessorDetailedResults');
		logger.info('Writing summary to ' + filename);
		
		resultsWriteStream = fs.createWriteStream(filename, {'flags' : 'w'});
		
		resultsWriteStream.on('open', function() {
			_.each(results, writeJobExecutions);
		});

		resultsWriteStream.on('finish', function() {
			logger.info('storeDetailedResults completed.');
			//callback(null, 'process');
			callback('storeResultsSummary');
		});
	};

	var storeReportsProcessorResultsSummary = function(callback) {
		var filename = createNextResultsFilename('reportProcessorSummary');

		logger.info('Writing summary to ' + filename);
		
		resultsWriteStream = fs.createWriteStream(filename, {'flags' : 'w'});
		
		resultsWriteStream.on('open', function() {
			_.each(results, writeJobExecutionsSummary);
		});

		resultsWriteStream.on('finish', function() {
			callback('storeReportsProcessorResultsSummary');
		});
	};
		
		
	var initialize = function(config) {
		configResultsDir = config.reportProcessor.resultsDir;

		fs.exists(configResultsDir, function (exists) {
			if (!exists) {
				fs.mkdirSync(configResultsDir);
			}
		});
		
		configStartTimeInMS = moment(config.reportProcessor.startDate, 'YYYY-MM-DD HH:mm:ss').unix();
		configEndTimeInMS = moment(config.reportProcessor.endDate, 'YYYY-MM-DD HH:mm:ss').unix();
	};
	
	
	var parseLogs = function(config, callback) {
		nextParserStates.push(readUntilStartOfJobExecution);
		
		var inputLogFiles = glob.sync(config.reportProcessor.inputLogFilesGlob);
		logger.debug(inputLogFiles);
		
		(function readNextFile() {
			(inputLogFiles.length) ? (function () {
				
				var nextFile = inputLogFiles.shift();
				logger.info('Next input log file : ' + nextFile);
				
				var stream = fs.createReadStream(nextFile);
				var streamSplitByCarriageReturns = stream.pipe(split());
				var parser = streamSplitByCarriageReturns.pipe(through(parseLine));
			
				parser.on('end', function() {
					logger.info('Finished reading ' + nextFile);
					readNextFile();
				});
			})() : storeResults(callback);
		})();
	};
	
	
	exports.process = function(config, callback) {
		initialize(config);
		
		/*
		 * This next call will one day fail because it will be called immediately after initialize() returns which may be before the 
		 * target directories have been created.  
		 * 
		 * Just something to keep in mind.
		 * Consider using Yield or Async or some home-grown flow-control.
		 */
		parseLogs(config, callback);
	};
};

exports.reportProcessor = new ReportProcessor();



