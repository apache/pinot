var URLUTILS = {
  forSegments: function(fabric, cluster, table) {
    return '/segments/'+fabric+'/'+cluster+'/'+table;
  },

  forSegmentsPage: function(fabric, cluster, table) {
    return '#'+this.forSegments(fabric, cluster, table);
  },

  forCluster: function(fabric, cluster) {
    return '/cluster/'+fabric+'/'+cluster;
  },

  forFabric: function(fabric) {
    return '/clusters/'+fabric;
  },

  forPql: function(fabric, pql) {
    return '/runpql/'+fabric+'?pql='+encodeURIComponent(pql);
  },

  forFabricsList: function() {
    return '/fabrics';
  },

  forAuthInfo: function() {
    return '/authinfo';
  },
};

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
  $.get(URLUTILS.forFabricsList(), function(data) {
    validateAjaxCall(data, function(data) {
      callback(data.fabrics);
    });
  });
}

var pinotApp = angular.module('pinotApp', ['ngRoute']);

pinotApp.config(function($routeProvider) {
  $routeProvider
    .when('/', {
      templateUrl: '/pinot_static/js/templates/fabric_list.html',
      controller: 'homeController'
    })
    .when('/fabric/:fabricName', {
      templateUrl: '/pinot_static/js/templates/cluster_list.html',
      controller: 'fabricController'
    })
    .when('/resource/:fabricName/:clusterName', {
      templateUrl: '/pinot_static/js/templates/cluster_profile.html',
      controller: 'resourceController'
    })
    .when('/segments/:fabricName/:clusterName/:tableName', {
      templateUrl: '/pinot_static/js/templates/table_segments.html',
      controller: 'segmentViewController'
    })
    .when('/console', {
      templateUrl: '/pinot_static/js/templates/query_console.html',
      controller: 'consoleController'
    })
});
