pinotApp.controller('resourceController', function($scope, $routeParams) {
  $scope.cluster = {
    'name': $routeParams.clusterName,
    'fabric': $routeParams.fabricName,
    'settings': {},
    'tables': [],
    'nodes': []
  }
  $scope.undecorate = undecorate;
  var noshow = ['tableName'];
  $.get(URLUTILS.forCluster($routeParams.fabricName, $routeParams.clusterName), function(data) {
    validateAjaxCall(data, function() {
      $scope.$apply(function() {
        _.each(data.info, function(v, k) {
          if (!_.contains(noshow, k))
            $scope.cluster.settings[k] = v;
        });

        _.each(data.tables, function(table) {
          $scope.cluster.tables.push({
            'name': table.name,
            'type': table.type,
            'url': URLUTILS.forTableInfo($routeParams.fabricName, $routeParams.clusterName, table.name)
          });
        });

        _.each(data.nodes, function(n, name) {
          $scope.cluster.nodes.push({
            'name': name,
            'type': n.type,
            'state': n.online ? 'Up' : 'Down',
            'label': n.online ? 'success' : 'alert',
          });
        })
      });
    });
  });
});
