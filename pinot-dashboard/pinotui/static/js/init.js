var URLUTILS = {
  forBackendTableInfo: function(fabric, cluster, table) {
    return '/cluster/'+fabric+'/'+cluster+'/table/'+table;
  },

  forTableInfo: function(fabric, cluster, table) {
    return '#'+this.forBackendTableInfo(fabric, cluster, table);
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

  forCreateTenant: function(fabric) {
    return '/create/tenant/'+fabric;
  },

  forCreateTable: function(fabric, resource) {
    return '/create/table/'+fabric+'/'+resource;
  },

};

function undecorate(name) {
  return name.split('_OFFLINE')[0].split('_ONLINE')[0].split('_BROKER')[0];
}

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
    .when('/cluster/:fabricName/:clusterName/table/:tableName', {
      templateUrl: '/pinot_static/js/templates/table_info.html',
      controller: 'tableInfoController'
    })
    .when('/fabric/:fabricName/createTenant', {
      templateUrl: '/pinot_static/js/templates/create_tenant.html',
      controller: 'createTenantController'
    })
    .when('/fabric/:fabricName/:clusterName/createTable', {
      templateUrl: '/pinot_static/js/templates/create_table.html',
      controller: 'createTableController'
    })
    .when('/fabric/:fabricName/:clusterName/createSchema', {
      templateUrl: '/pinot_static/js/templates/create_schema.html',
      controller: 'createSchemaController'
    })
    .when('/console', {
      templateUrl: '/pinot_static/js/templates/query_console.html',
      controller: 'consoleController'
    })
});
