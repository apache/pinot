pinotApp.controller('fabricController', function($scope, $routeParams) {
  $scope.resources = [];
  var fabric = $routeParams.fabricName;
  $scope.fabric = fabric;
  $scope.pageName = 'Resources in '+fabric;
  $.get(URLUTILS.forFabric(fabric), function(data) {
    validateAjaxCall(data, function() {
      $scope.$apply(function() {
        $scope.resources = data.clusters;
      });
    });
  });
});

