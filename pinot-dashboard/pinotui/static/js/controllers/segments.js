pinotApp.controller('segmentViewController', function($scope, $routeParams) {
  $scope.cluster = {
    'name': $routeParams.clusterName,
    'fabric': $routeParams.fabricName,
    'table': $routeParams.tableName,
    'segments': [],
  }
  $.get(URLUTILS.forSegments($routeParams.fabricName, $routeParams.clusterName, $routeParams.tableName), function(data) {
    validateAjaxCall(data, function() {
      $scope.$apply(function() {
        $scope.cluster.segments = _.pluck(data.segments, 'name');
      });
    });
  });
});

