"use strict";

var page = require('webpage').create();
var system = require('system');
var args = require('system').args;
var options = {
  anomalyRoute: args[1],
  output: args[2],
  width: args[3],
  height: args[4],
};

page.viewportSize = {
  width: options.width || 800,
  height: options.height || 1600
};
page.settings.resourceTimeout = 1000000;

var requestsArray = [];

// recording navigation request
page.onNavigationRequested = function(url, type, willNavigate, main) {
  system.stderr.writeLine('= onNavigationRequested()');
  system.stderr.writeLine(' Trying to navigate to: ' + url);
  system.stderr.writeLine(' Caused by: ' + type);
  system.stderr.writeLine(' Will actually navigate: ' + willNavigate);
  system.stderr.writeLine(' Sent from the page\'s main frame: ' + main);
};

// recording resource loading errors; exit when operations are cancelled
page.onResourceError = function(resourceError) {
  system.stderr.writeLine('= onResourceError()');
  system.stderr.writeLine(' Unable to load resource (#' + resourceError.id + 'URL:' + resourceError.url + ')');
  system.stderr.writeLine(' Error code: ' + resourceError.errorCode + '. Description: ' + resourceError.errorString);
};

// on resource requested
page.onResourceRequested = function(requestData, networkRequest) {
  system.stderr.writeLine('= onResourceRequested()');
  requestsArray.push(requestData.id);
  system.stderr.writeLine(requestsArray.length + ' pending requests');
  system.stderr.writeLine(' Request (#' + requestData.id + '): ' + 'url ' +requestData.url);
};

page.onResourceReceived = function (response) {
  var index = requestsArray.indexOf(response.id);
  if (index !== -1) {
    requestsArray.splice(index, 1);
  }
  system.stderr.writeLine('= onResourceReceived()');
  system.stderr.writeLine(' Request (#' + response.id + '): ');
  system.stderr.writeLine(requestsArray.length + ' pending requests');
};


page.open(options.anomalyRoute, function(status) {
  system.stderr.writeLine('= onOpen()');
  system.stderr.writeLine(' PhantomJS version: ' + JSON.stringify(phantom.version));
  system.stderr.writeLine(' PhantomJS page settings: ' + JSON.stringify(page.settings));
  if (status !== 'success') {
   system.stderr.writeLine('Error opening url');
   phantom.exit(1);
  }
});

page.onLoadFinished = function(status) {
  system.stderr.writeLine('= onLoadFinished()');
  console.log('requestedArray');
  console.log(requestsArray.length);

  var interval = setInterval(function () {

    if (requestsArray.length === 0) {
      clearInterval(interval);
      var clipRect = page.evaluate(function () {
        return document.querySelector('#anomaly-chart-0').getBoundingClientRect();
      });
      page.clipRect = {
        top:    clipRect.top + 10,
        left:   clipRect.left + 15,
        width:  clipRect.width,
        height: clipRect.height
      };
      page.render(options.output);
      phantom.exit();
    }
  }, 500);

  interval();

};
