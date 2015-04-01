function getFabrics(callback) {
  $.get('/fabrics', function(data) {
    callback(data.fabrics);
  });
}


var pinotApp = angular.module('pinotApp', ['ngRoute']);

pinotApp.controller('TopBarController', function($scope) {
  $scope.fabrics = [];
  getFabrics(function(fabrics) {
    $scope.$apply(function() {
      $scope.fabrics = fabrics;
    });
  });
});

pinotApp.config(function($routeProvider) {
  $routeProvider
    .when('/', {
      templateUrl: '/static/partials/fabric_list.html',
      controller: 'homeController'
    })
    .when('/fabric/:fabricName', {
      templateUrl: '/static/partials/cluster_list.html',
      controller: 'fabricController'
    })
    .when('/resource/:fabricName/:clusterName', {
      templateUrl: '/static/partials/cluster_profile.html',
      controller: 'resourceController'
    })
    .when('/segments/:fabricName/:clusterName/:tableName', {
      templateUrl: '/static/partials/table_segments.html',
      controller: 'segmentViewController'
    })
    .when('/console', {
      templateUrl: '/static/partials/query_console.html',
      controller: 'consoleController'
    })
});

pinotApp.controller('homeController', function($scope) {
  $scope.fabrics = [];
  getFabrics(function(fabrics) {
    $scope.$apply(function() {
      $scope.fabrics = fabrics;
      window.location = '#/fabric/'+fabrics.slice(0, 1).toString();
    });
  });
  $scope.pageName = 'Fabric List';
});

pinotApp.controller('fabricController', function($scope, $routeParams) {
  $scope.resources = [];
  var fabric = $routeParams.fabricName;
  $scope.fabric = fabric;
  $scope.pageName = 'Resources in '+fabric;
  $.get('/clusters/'+fabric, function(data) {
    $scope.$apply(function() {
      $scope.resources = data.clusters;
    });
  });
});

pinotApp.controller('consoleController', function($scope) {
  $scope.fabrics = []
  $scope.pageName = 'Query Console';
  $scope.queryConsole = true;

  var editor = CodeMirror.fromTextArea(document.getElementById('cm_prompt'), {
    lineWrapping: true,
    lineNumbers: true,
    mode: "sql",
    theme: "elegant"
  });

  var result = CodeMirror.fromTextArea(document.getElementById('cm_result'), {
    lineWrapping: true,
    lineNumbers: true,
    mode: "javascript",
    theme: "elegant",
    readOnly: true,
    smartIndent: true,
  });

  var fabric_menu = $('#pick_fabric');

  getFabrics(function(fabrics) {
    $scope.$apply(function() {
      $scope.fabrics = fabrics;
    });
  });

  $('#run_code').click(function() {
    var pql = editor.getValue().trim();
    var fabric = fabric_menu.val();

    if (fabric == '') {
      alert('pick a fabric');
      return;
    }

    if (pql == '') {
      alert('type a query');
      return;
    }

    $.get('/runpql/'+fabric+'?pql='+encodeURIComponent(pql), function(data) {
      if (data.success) {
        result.setValue(JSON.stringify(data.result));
      }
      else {
        result.setValue('Failed: '+data.result);
      }
    })

    return false;
  });

});

pinotApp.controller('segmentViewController', function($scope, $routeParams) {
  $scope.cluster = {
    'name': $routeParams.clusterName,
    'fabric': $routeParams.fabricName,
    'table': $routeParams.tableName,
    'segments': [],
  }
  $.get('/segments/'+$routeParams.fabricName+'/'+$routeParams.clusterName+'/'+$routeParams.tableName, function(data) {
    $scope.$apply(function() {
      $scope.cluster.segments = data.segments;
    });
  });
});

pinotApp.controller('resourceController', function($scope, $routeParams) {
  $scope.cluster = {
    'name': $routeParams.clusterName,
    'fabric': $routeParams.fabricName,
    'settings': {},
    'tables': [],
    'nodes': []
  }
  var noshow = ['tableName'];
  $.get('/cluster/'+$routeParams.fabricName+'/'+$routeParams.clusterName, function(data) {
    $scope.$apply(function() {
      _.each(data.info, function(v, k) {
        if (!_.contains(noshow, k))
          $scope.cluster.settings[k] = v;
      });

      if (_.has(data.info, 'tableName')) {
        _.each(data.info.tableName, function(table) {
          $scope.cluster.tables.push({
            'name': table,
            'url': '#/segments/'+$routeParams.fabricName+'/'+$routeParams.clusterName+'/'+table
          });       
        });
      }

      _.each(data.nodes, function(n, name) {
        $scope.cluster.nodes.push({
          'name': name,
          'state': n.online ? 'Up' : 'Down',
          'label': n.online ? 'success' : 'alert',
        });     
      })
    });
  });
});
