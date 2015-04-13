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
    validateAjaxCall(data, function() {
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
});
