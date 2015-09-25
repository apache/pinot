'use strict';

/* Controllers */

var thirdEyeDetectorControllers = angular.module('thirdEyeDetectorControllers', []);

thirdEyeDetectorControllers.controller('TimeSeriesCtrl', ['$scope', '$routeParams', 'TimeSeries',
  function($scope, $routeParams, TimeSeries) {
    $scope.isLoaded = false;
    $scope.timeSeries = TimeSeries.query(function(res) {
      $scope.isLoaded = true;

      // Convert milliseconds to dates for series
      for (var i = 0; i < res.data.length; i++) {
        for (var j = 0; j < res.data[i].length; j++) {
          res.data[i][j]['time'] = new Date(res.data[i][j]['time']);
        }
      }

      // Convert milliseconds to dates for markers
      for (var i = 0; i < res.markers.length; i++) {
        res.markers[i]['time'] = new Date(res.markers[i]['time']);
      }

      // Add nicely parsed properties
      for (var i = 0; i < res.anomaly_results.length; i++) {
        res.anomaly_results[i].parsedFunctionProperties = res.anomaly_results[i].functionProperties.split(";");
        res.anomaly_results[i].parsedProperties = res.anomaly_results[i].properties.split(";");
      }

      res.target = '#time-series';
      res.legend_target = '#time-series-legend';
      res.width = $('#time-series-display').width();
      res.height = 300;
      MG.data_graphic(res);
    });
  }
]);