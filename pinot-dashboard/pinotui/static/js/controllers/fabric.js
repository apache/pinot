pinotApp.controller('fabricController', function($scope, $routeParams) {
  $scope.resources = [];
  $scope.nodes = [];
  var fabric = $routeParams.fabricName;
  $scope.fabric = fabric;
  $scope.pageName = 'Resources in '+fabric;
  $.get(URLUTILS.forFabric(fabric), function(data) {
    validateAjaxCall(data, function() {
      $scope.$apply(function() {
        $scope.resources = data.clusters;
        $scope.nodes = data.nodes;
      });
    });
  });
});

