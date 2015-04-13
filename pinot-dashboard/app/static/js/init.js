function validateAjaxCall(data, callback) {
  if (data.success) {
    callback(data)
  }
  else {
    $('#alerts').prepend(
      $('<div>')
        .addClass('alert-box alert')
        .attr('data-alert', '')
        .text(data.error_message)
        .append(
          $('<a>')
            .addClass('close')
            .html('&times;')
            .attr('href', '#')
            .click(function() {
              return false;
            })
          )
    );
  }
}

function getFabrics(callback) {
  $.get('/fabrics', function(data) {
    validateAjaxCall(data, function(data) {
      callback(data.fabrics);
    });
  });
}

var pinotApp = angular.module('pinotApp', ['ngRoute']);

pinotApp.config(function($routeProvider) {
  $routeProvider
    .when('/', {
      templateUrl: '/static/js/templates/fabric_list.html',
      controller: 'homeController'
    })
    .when('/fabric/:fabricName', {
      templateUrl: '/static/js/templates/cluster_list.html',
      controller: 'fabricController'
    })
    .when('/resource/:fabricName/:clusterName', {
      templateUrl: '/static/js/templates/cluster_profile.html',
      controller: 'resourceController'
    })
    .when('/segments/:fabricName/:clusterName/:tableName', {
      templateUrl: '/static/js/templates/table_segments.html',
      controller: 'segmentViewController'
    })
    .when('/console', {
      templateUrl: '/static/js/templates/query_console.html',
      controller: 'consoleController'
    })
});
