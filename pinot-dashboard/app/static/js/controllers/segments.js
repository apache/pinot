pinotApp.controller('segmentViewController', function($scope, $routeParams) {
  $scope.cluster = {
    'name': $routeParams.clusterName,
    'fabric': $routeParams.fabricName,
    'table': $routeParams.tableName,
    'segments': [],
  }
  $.get('/segments/'+$routeParams.fabricName+'/'+$routeParams.clusterName+'/'+$routeParams.tableName, function(data) {
    validateAjaxCall(data, function() {
      $scope.$apply(function() {
        $scope.cluster.segments = data.segments;
      });
    });
  });
});

