pinotApp.controller('tableInfoController', function($scope, $routeParams) {
  $scope.cluster = {
    'name': $routeParams.clusterName,
    'fabric': $routeParams.fabricName,
    'table': $routeParams.tableName,
    'segments': [],
    'configs': [],
  }
  $scope.undecorate = undecorate;
  $.get(URLUTILS.forBackendTableInfo($routeParams.fabricName, $routeParams.clusterName, $routeParams.tableName), function(data) {
    validateAjaxCall(data, function() {
      console.log(data)
      $scope.$apply(function() {
        $scope.cluster.segments = _.pluck(data.segments, 'info');
        $scope.cluster.configs = data.data;
      });
    });
  });
});
