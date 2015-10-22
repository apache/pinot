'use strict';

/* App Module */

var thirdEyeDetectorApp = angular.module('thirdEyeDetectorApp', [
  'ngRoute',
  'thirdEyeDetectorControllers',
  'thirdEyeDetectorServices'
]);

thirdEyeDetectorApp.config(['$routeProvider',
  function($routeProvider) {
    $routeProvider.
      when('/time-series/:collection/:metric/:start/:end', {
        templateUrl: 'partials/time-series.html',
        controller: 'TimeSeriesCtrl'
      }).
      otherwise({
        redirectTo: '/'
      });
  }]);