// screenshot script for reference only
// actual used script lives in /thirdeye-configs/prod-config
"use strict";

var page = require('webpage').create();
var system = require('system');
var args = require('system').args;
var isReady = false;
var options = {
  anomalyRoute: args[1],
  output: args[2],
  width: args[3],
  height: args[4],
  cookieName: args[5],
  cookieValue: args[6],
  domain: args[7]
};

page.viewportSize = {
  width: options.width || 900,
  height: options.height || 1600
};
page.settings.resourceTimeout = 180000;
page.onResourceTimeout = function(error) {
  phantom.exit(1);
};
page.addCookie({
  'name'   : options.cookieName,
  'value'  : options.cookieValue,
  'domain' : options.domain
});
var requestsArray = [];


// recording resource loading errors; exit when operations are cancelled
page.onResourceError = function(resourceError) {
  system.stderr.writeLine('= onResourceError()');
  system.stderr.writeLine(' Unable to load resource (#' + resourceError.id + 'URL:' + resourceError.url + ')');
  system.stderr.writeLine(' Error code: ' + resourceError.errorCode + '. Description: ' + resourceError.errorString);
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

page.onCallback = function(data) {
  console.log(data);
  if (data.message === 'ready') {
    system.stderr.writeLine('= onCallBack() && isReady');
    isReady = true;
  }
};

page.onLoadFinished = function(status) {
  system.stderr.writeLine('= onLoadFinished()');
  console.log('requestedArray');
  console.log(requestsArray.length);

  var interval = setInterval(function () {

    if (requestsArray.length === 0 && isReady) {
      system.stderr.writeLine('taking screenshot');

      clearInterval(interval);
        var clipRect = page.evaluate(function () {
          return document.querySelector('.timeseries-chart').getBoundingClientRect();
        });
        page.clipRect = {
          top:    clipRect.top,
          left:   clipRect.left,
          width:  clipRect.width + 20,
          height: clipRect.height
        };
        page.render(options.output);
        phantom.exit();
    }
  }, 10000);

  interval();

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
