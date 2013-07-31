var reportProcessor = require('../lib/reportProcessor');

var callback = function (msg1, msg2) {
	'use strict';
	console.log (msg1 + '\n' + msg2);
	return;
};


describe('Report Processor Tests', function() {
	it("should respond with hello world", function(done) {
		expect(reportProcessor.getStartTime(null)).toEqual("hello world");
	});
});
    

