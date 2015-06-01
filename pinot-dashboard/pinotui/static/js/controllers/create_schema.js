pinotApp.controller('createSchemaController', function($scope, $routeParams) {
  $scope.cluster = {
    'name': $routeParams.clusterName,
    'fabric': $routeParams.fabricName,
  }

  $scope.create = function(data) {
    console.log(data)
  }
});

