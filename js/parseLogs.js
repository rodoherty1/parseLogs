#!/usr/bin/env node

var async = require('async'),
	reportProcessor = require('./lib/reportProcessor'),
	optimist = require('optimist')
    .usage('Reads some output files and generates a log.\nUsage: $0 -c [config]')
    .alias('c', 'config')
    .describe('c', 'Config file describing the locations of the input files and directories')
	.demand(['c']).
	check(function (argv) {
		'use strict';
		return fs.existsSync(argv.config);
		}),
	fs = require('fs'),
	$ = require('jquery');

var App = function () {
	'use strict';

	var that = this;
	
	var config = {};

    /**
     *  terminator === the termination handler
     *  Terminate server on receipt of the specified signal.
     *  @param {string} sig  Signal to terminate on.
     */
    var terminator = function(sig){
        if (typeof sig === "string") {
           console.log('%s: Received %s - terminating sample app ...',
                       Date(Date.now()), sig);
           process.exit(1);
        }
        console.log('UpdateRss3App stopped.');
    };
    
    
    /**
     *  Setup termination handlers (for exit and a list of signals).
     */
    var setupTerminationHandlers = function() {
        //  Process on exit and signals.
        process.on('exit', function() {
			console.log ('exit event - calling terminator()');
			terminator();
		});

        ['SIGHUP', 'SIGINT', 'SIGQUIT', 'SIGILL', 'SIGTRAP', 'SIGABRT',
         'SIGBUS', 'SIGFPE', 'SIGUSR1', 'SIGSEGV', 'SIGUSR2', 'SIGTERM'
        ].forEach(function(element, index, array) {
            process.on(element, function() {
				terminator(element); });
			});
    };

	var checkArgs = function(callback) {
		var stream = fs.createReadStream(optimist.argv.config);
		
		var lines = '';
		
		stream.on('data', function (buf) {
		    lines += buf;
		});

		stream.on('end', function () {
			config = $.parseJSON(lines);
			callback(null, 'checkArgs');
		});	
	};
	
	this.initialize = function() {
		setupTerminationHandlers();
	};

	var processLogs = function(callback) {
		reportProcessor.process(config, callback);
	};
	

	
	this.start = function() {
		async.series([
			checkArgs,
			processLogs
		],
		function(err, results){
			if (err) {
				console.log('Errors encountered - ' + err);
			}
			
			console.log(results);
		});
	};
	

}; /* end App declaration */


var app = new App();
app.initialize();
app.start();





