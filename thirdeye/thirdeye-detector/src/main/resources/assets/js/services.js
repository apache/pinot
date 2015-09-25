'use strict';

/* Services */

var thirdEyeDetectorServices = angular.module('thirdEyeDetectorServices', ['ngResource']);

thirdEyeDetectorServices.factory('TimeSeries', ['$resource', '$routeParams',
  function($resource, $routeParams) {
    var uri = 'api/time-series/metrics-graphics/:collection/:metric/:start/:end';

    var queryParams = []
    if ($routeParams['groupBy']) {
        queryParams.push('groupBy=:groupBy');
    }
    if ($routeParams['topK']) {
        queryParams.push('topK=:topK');
    }
    if ($routeParams['functionId']) {
        queryParams.push('functionId=:functionId');
    }
    if (queryParams.length > 0) {
        uri += '?' + queryParams.join('&');
    }

    return $resource(uri, {}, {
      query: { method: 'GET', params: $routeParams, isArray: false }
    });
  }]);